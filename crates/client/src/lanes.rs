//! One fetch's paid lanes (ADR 039): building a provider's lane on the run's
//! pool (`build_multi_lane`), and persisting what each lane paid once the
//! fetch ends (`queue_face_watermarks`).
//!
//! A lane is one `(pool, signer, provider)` voucher sequence. It resumes from
//! the buyer store's watermark, so its first voucher continues the lane. A
//! self-owned lane the store has none for resumes from the chain's; a
//! delegated one starts at zero and heals from the node's watermark bundle. Every lane settles at its armed
//! cumulative, which equals its committed one on the clean path.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use decdn_incentive::buyer_pool::{AdvanceOutcome, BuyerPoolStore};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{CapabilityGrant, LaneKey, PoolId};
use iroh::{Endpoint, EndpointAddr, RelayUrl};
use tokio_util::sync::CancellationToken;

use crate::buyer_pool::{OwnedPayer, ProgressWrite, WatermarkRow};
use crate::discovery::{self, NodeCandidate};
use crate::funding::{LaneKeys, RunFunding, build_ctx_for_fetch};
use crate::ordered_writes::OrderedWrites;
use crate::session::{ChainConfig, DepositPolicy, FetchMarks, PullSite};
use crate::{
    Connections, CredentialSlot, Cumulative, LaneHandle, LaneLedgers, PeerSource, PoolContext,
    PoolLedger, PullDeadlines, VoucherProgress,
};

/// Settles a drive that is dropped before it returns, as a Ctrl-C does.
///
/// A drive persists its lanes' voucher watermarks after it returns. A dropped
/// drive never gets there, so this guard runs `settle` from its `Drop` instead:
/// the lanes' paid bytes are queued for recording, at the armed (HIGH)
/// settlement (`queue_face_watermarks`), and the next run's vouchers continue
/// from them. A drive that returns calls [`Self::disarm`] and settles on its
/// normal path.
pub struct SettleOnDrop<F: FnOnce()> {
    /// The settle to run on drop; `None` once disarmed.
    settle: Option<F>,
}

impl<F: FnOnce()> std::fmt::Debug for SettleOnDrop<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettleOnDrop")
            .field("armed", &self.settle.is_some())
            .finish()
    }
}

impl<F: FnOnce()> SettleOnDrop<F> {
    /// Arm `settle` for a drop before [`Self::disarm`].
    pub const fn new(settle: F) -> Self {
        Self {
            settle: Some(settle),
        }
    }

    /// The drive returned: its normal path settles, so the drop does nothing.
    pub fn disarm(mut self) {
        self.settle = None;
    }
}

impl<F: FnOnce()> Drop for SettleOnDrop<F> {
    fn drop(&mut self) {
        if let Some(settle) = self.settle.take() {
            settle();
        }
    }
}

/// Persist what the pool lane paid, warning rather than masking the fetch
/// outcome.
///
/// Shared by the single- and multi-source paths: the bytes were paid for either
/// way, and a failure to record that only risks a rejected reuse next time.
fn persist_watermark(
    store: &dyn BuyerPoolStore,
    row: WatermarkRow,
    lane: LaneKey,
    progress: &VoucherProgress,
) {
    let Some(write) = ProgressWrite::of(progress) else {
        return;
    };
    let label = match write {
        ProgressWrite::Rebase { .. } => "rebased",
        ProgressWrite::Advance { .. } => "advanced",
    };
    let outcome = write.apply(store, row, lane);
    // A non-`Advanced` outcome (unknown pool / replaced owner slot / regression)
    // means the watermark did NOT move — same hazard as a backend error — so
    // surface it too rather than dropping it on the floor. A lost rebase write
    // heals itself: the next run signs from the old anchor, draws `Underpaid`
    // again, and rebases again.
    let (bytes_delivered, amount) = progress.totals();
    let pool_id = lane.pool_id;
    match outcome {
        Ok(AdvanceOutcome::Advanced) => {}
        Ok(other) => tracing::warn!(
            write = label,
            %bytes_delivered,
            %amount,
            "{label} voucher watermark not persisted for pool {pool_id} (provider {}): \
             {other:?}; the next reuse may re-sign a stale watermark, which that provider \
             rejects",
            lane.provider
        ),
        Err(e) => tracing::warn!(
            write = label,
            %bytes_delivered,
            %amount,
            "failed to persist {label} voucher watermark for pool {pool_id} (provider {}): \
             {e}; the next reuse may re-sign a stale watermark, which that provider rejects",
            lane.provider
        ),
    }
}

/// The per-stream idle floor inside one paid leg: a leg whose stream opens no
/// byte, or delivers none, for this long ends as a source fault. The acquire
/// loop's lane watchdog ([`crate::LANE_WATCHDOG`]) covers a lane that
/// makes slow progress.
pub const STALL_WINDOW: Duration = Duration::from_secs(30);

/// What a lane's vouchers sign under.
#[derive(Debug, Clone, Copy)]
pub enum LanePayer<'a> {
    /// The buyer's own pool: the payer's owner key owns it, and its lane key
    /// signs.
    Owned(&'a OwnedPayer),
    /// A pool another owner funds, signed under the slot's credential as it is
    /// when the lane is built.
    Delegated(&'a CredentialSlot),
}

/// Shared pull/funding deps every lane of one fetch borrows for its lifetime
/// (`build_multi_lane`). A blob fetch and every entry of a bundle pull
/// build their lanes from the same shape. Every field is a borrow or a `Copy`
/// scalar.
pub struct LaneDeps<'a, P> {
    /// The endpoint every lane dials from, and whose node id the client
    /// identity binding signs.
    pub endpoint: &'a Endpoint,
    /// The buyer-pool store each lane reuses its pool from and records its
    /// watermark in. Shared, so a lane's watermark write runs on the blocking
    /// pool (`queue_face_watermarks`).
    pub store: &'a Arc<dyn BuyerPoolStore>,
    /// The `PaymentPool` contract the pool lives on.
    pub contract: &'a PaymentPool::PaymentPoolInstance<P>,
    /// The wallet-filled provider the open, top-up and allowance transactions
    /// go through.
    pub rpc: &'a P,
    /// The `SlashJudge` domain every stream response verifies against.
    pub slash_dom: &'a Eip712Domain,
    /// The buyer's own address: the pool owner on the self-owned path, the
    /// delegate signer on the delegated one. On the self-owned path it need not
    /// be the key the lanes sign under ([`OwnedPayer::lane`]).
    pub self_address: Address,
    /// The pool's settlement token (USDC), read once via `PaymentPool.usdc()`
    /// and threaded through rather than re-read per top-up.
    pub token: Address,
    /// The chain the pull pays on.
    pub chain: &'a ChainConfig,
    /// Where the pull keeps its peer store and scratch files.
    pub site: &'a PullSite,
    /// How a self-owned pool is funded.
    pub deposit: DepositPolicy,
    /// The ADR 005 namespace routing hint every request carries.
    pub namespace_id: [u8; 32],
    /// The highest per-MB rate a lane accepts; `0` sets no ceiling.
    pub max_rate_per_mb: u64,
    /// The largest blob a lane accepts.
    pub max_blob_bytes: u64,
    /// Each leg's open and idle deadlines.
    pub deadlines: PullDeadlines,
    /// The command's one connection per node, shared by every lane of every
    /// fetch the command runs.
    pub connections: &'a Connections,
    /// The command's peer-record and watermark writes, run off the runtime
    /// thread in queue order across every fetch the command runs. Every
    /// ordered state write of a fetch goes through it.
    pub writes: &'a OrderedWrites,
    /// The run's funding facts, shared by every lane build and top-up of the
    /// run ([`RunFunding`]).
    pub funding: &'a RunFunding,
    /// Where each built lane is reported for a time-to-first-byte log; `None`
    /// reports nothing.
    pub marks: Option<&'a dyn FetchMarks>,
}

impl<P> std::fmt::Debug for LaneDeps<'_, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneDeps")
            .field("self_address", &self.self_address)
            .field("chain", &self.chain)
            .field("deposit", &self.deposit)
            .finish_non_exhaustive()
    }
}

/// The lane's shared ledger + context: from the run registry when bundle pull
/// supplies one (so every entry/chunk on the lane shares one monotonic issuer),
/// else a fresh pair for a standalone blob fetch.
///
/// On a registry reuse (an already-registered lane, second+ touch), `ctx` — this
/// call's freshly built context, which may reflect an `open_or_reuse_pool`
/// low-water top-up the registry's stored context predates — is otherwise
/// discarded in favor of the shared handle. Deposit only ever grows via
/// top-ups and the on-chain deposit is the hard backstop, so raising the
/// shared handle's `deposit` toward this call's fresh reading is always safe;
/// reconciling it here stops the pool-wide gate from reading a stale, lower
/// deposit and refusing prematurely (`PoolExhausted`) once the true balance
/// has grown. The reconcile also runs (as a no-op) when this call's build won
/// the registry race, since the shared value already equals the fresh one.
fn lane_ledger(
    ledgers: Option<&LaneLedgers>,
    lane: LaneKey,
    ctx: PoolContext,
) -> (Arc<PoolLedger>, Arc<Mutex<PoolContext>>) {
    let seed = Cumulative {
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    };
    // `U256` is `Copy`; capture the fresh deposit before `ctx` moves into the
    // `get_or_insert` build closure below.
    let fresh_deposit = ctx.deposit;
    match ledgers {
        Some(reg) => {
            let h = reg.get_or_insert(lane, || LaneHandle {
                ledger: Arc::new(PoolLedger::new(seed)),
                ctx: Arc::new(Mutex::new(ctx)),
            });
            {
                let mut g = h.ctx.lock().unwrap_or_else(PoisonError::into_inner);
                g.deposit = g.deposit.max(fresh_deposit);
            }
            (h.ledger, h.ctx)
        }
        None => (Arc::new(PoolLedger::new(seed)), Arc::new(Mutex::new(ctx))),
    }
}

/// One built payment lane: the per-provider `PoolContext`/`PoolLedger` and the
/// [`PeerSource`] that pays with them. [`crate::lane_sources::LaneSources`] moves
/// the source into the [`crate::StreamCandidate`] the acquire loop
/// consumes and keeps
/// a [`FaceLaneHandle`] on the same `ctx`/`ledger` for the watermark.
pub(crate) struct MultiLane<'a> {
    pub(crate) provider: Address,
    /// The key the lane's vouchers sign under.
    pub(crate) signer: Address,
    pub(crate) pool_id: PoolId,
    /// The lane's persisted prior amount — the baseline
    /// [`persist_watermark`] computes the advance against.
    pub(crate) prior_amount: U256,
    pub(crate) ctx: Arc<Mutex<PoolContext>>,
    pub(crate) ledger: Arc<PoolLedger>,
    /// The lane's source.
    pub(crate) source: PeerSource<'a>,
}

/// Build the target address for a candidate: its `node_id`, the pinned
/// `addr` when the user gave one (`--addr`, which requires `--node-id`), the
/// first configured relay hint, and its registry multiaddrs as direct-address
/// hints (ADR 001 § Node Discovery).
///
/// iroh sends the first handshake packet to every path the target carries, so
/// the relay hint and the direct addresses race from the start: a reachable
/// node settles on the direct path, and a node behind NAT connects over the
/// relay with no wait for the pkarr lookup. A hint that names a relay other
/// than the node's home relay drops its packets, and the lookup supplies the
/// right one.
pub(crate) fn lane_target(
    candidate: &NodeCandidate,
    addr: Option<std::net::SocketAddr>,
    relays: &[RelayUrl],
) -> EndpointAddr {
    let mut target = EndpointAddr::new(candidate.node_id);
    if let Some(addr) = addr {
        target = target.with_ip_addr(addr);
    }
    if let Some(url) = relays.first() {
        target = target.with_relay_url(url.clone());
    }
    // Registry multiaddrs as direct-address hints: a reachable lane provider
    // connects without a relay (ADR 001 § Node Discovery).
    discovery::with_dial_addrs(target, candidate)
}

/// Build one [`MultiLane`] for `provider` at `target`: open/reuse the pool,
/// seed a ledger from that lane's persisted cumulative, and wrap a
/// [`PeerSource`] over it.
///
/// `open_lock`, when `Some`, serializes the pool open-or-reuse inside
/// [`build_ctx_for_fetch`]: across one fetch's concurrently built lanes, which
/// all open or reuse the one on-chain pool, and across other fetches sharing
/// that pool (bundle pull's cross-entry concurrency, #1774); the guard is dropped before
/// any streaming, and skipped entirely on the delegated path, which opens
/// nothing on-chain. A caller that holds a provider's stream permit takes it
/// before `open_lock`, so the lock order stays permit → `open_lock` and no
/// hold-and-wait cycle can form.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_multi_lane<'a, P>(
    deps: &LaneDeps<'a, P>,
    payer: LanePayer<'_>,
    voucher_dom: &Eip712Domain,
    provider: Address,
    target: EndpointAddr,
    open_lock: Option<&tokio::sync::Mutex<()>>,
    ledgers: Option<&LaneLedgers>,
    ended: &CancellationToken,
) -> anyhow::Result<MultiLane<'a>>
where
    P: alloy::providers::Provider + Clone,
{
    // A delegated lane signs under the slot's credential as it is now: a lane
    // built after a swap signs under the new key and capability.
    let keys = match payer {
        LanePayer::Owned(payer) => LaneKeys::Owned(payer),
        LanePayer::Delegated(slot) => {
            let credential = slot.current();
            LaneKeys::Delegated {
                grant: CapabilityGrant::from_signed_capability(&credential.capability),
                signer: credential.signer,
            }
        }
    };
    let (ctx, spend) = {
        // Bundle pull funnels every entry — and every lane of a multi-source
        // entry — through the one shared `PaymentPool` deposit, so the
        // open-or-reuse serializes across the whole bundle. The delegated path
        // opens nothing on-chain (`build_delegated_pool_ctx` adopts the owner's
        // pool), so it skips the guard: holding the bundle-wide lock there
        // would serialize unrelated fetches for no mutual-exclusion win.
        let _open_guard = match (open_lock, &keys) {
            (Some(lock), LaneKeys::Owned(_)) => Some(lock.lock().await),
            _ => None,
        };
        build_ctx_for_fetch(deps, &keys, voucher_dom, provider, ended).await?
    };
    let pool_id = ctx.pool_id;
    let prior_amount = ctx.prior_amount;
    let lane = LaneKey {
        pool_id,
        signer: keys.lane_signer().address(),
        provider,
    };
    // The lane's ledger + context, seeded from its persisted `(signer,
    // provider)` cumulative so the first voucher continues the lane (a restart
    // from zero is rejected as a regression) — from the run registry when
    // `ledgers` is `Some`, else a fresh pair.
    let (ledger, ctx) = lane_ledger(ledgers, lane, ctx);
    // The run's view of the pool's spend counts this lane through its ledger
    // from now on, whether or not the acquire loop ever starts it. A delegated
    // lane draws on a pool this caller does not own, and joins nothing.
    if let Some(spend) = spend {
        deps.funding.join_lane(lane, spend, &ledger);
    }
    let source = lane_source(
        deps,
        target,
        Arc::clone(&ctx),
        Arc::clone(&ledger),
        provider,
    );
    if let Some(marks) = deps.marks {
        marks.lane_built();
    }
    Ok(MultiLane {
        provider,
        signer: lane.signer,
        pool_id,
        prior_amount,
        ctx,
        ledger,
        source,
    })
}

/// `provider`'s lane source at `target`, paying through `ctx` and `ledger`.
/// Every open takes a stream on the command's connection to `target`
/// ([`LaneDeps::connections`]).
fn lane_source<'a, P>(
    deps: &LaneDeps<'a, P>,
    target: EndpointAddr,
    ctx: Arc<Mutex<PoolContext>>,
    ledger: Arc<PoolLedger>,
    provider: Address,
) -> PeerSource<'a> {
    PeerSource::new(
        deps.endpoint,
        target,
        ctx,
        ledger,
        deps.slash_dom,
        provider,
        deps.namespace_id,
        deps.max_blob_bytes,
        deps.max_rate_per_mb,
        deps.deadlines,
        Some(deps.connections.clone()),
    )
}

/// One lane's watermark-persistence handle: what [`stream_lane_watermarks`]
/// needs to settle the lane AFTER the fetch, kept alive after the lane's owned
/// [`PeerSource`] has moved into its [`crate::StreamCandidate`]. The
/// `ledger` and
/// `ctx` are the SAME `Arc`s the fetch paid through, so the lane's settlement
/// is read here once the fetch ends.
pub(crate) struct FaceLaneHandle {
    pub(crate) pool_id: PoolId,
    pub(crate) provider: Address,
    /// The key the lane's vouchers sign under: its lane's `signer`.
    pub(crate) signer: Address,
    pub(crate) prior_amount: U256,
    pub(crate) ledger: Arc<PoolLedger>,
    /// The lane's buyer context, read to annotate an unbound cache miss.
    pub(crate) ctx: Arc<Mutex<PoolContext>>,
}

/// The per-lane [`LaneWatermark`]s to persist after a face fetch, read from the
/// retained [`FaceLaneHandle`]s — the same settle-at-armed-cumulative rule
/// [`multi_lane_watermarks`] then keys for persistence.
///
/// One watermark per ledger: a lane rebuilt on the run's shared ledger leaves a
/// second handle on it, and the first read takes the ledger's unsaved rebase,
/// so a second write for it would only repeat the first. Each rebuild read its
/// own baseline from the store, and a sibling entry's rebase can have moved
/// that baseline down since the first build, so the watermark takes the lowest
/// one: the advance past it still persists.
fn stream_lane_watermarks(lanes: &[FaceLaneHandle]) -> Vec<LaneWatermark> {
    let mut per_ledger: Vec<(&FaceLaneHandle, U256)> = Vec::new();
    for l in lanes {
        match per_ledger
            .iter_mut()
            .find(|(first, _)| Arc::ptr_eq(&first.ledger, &l.ledger))
        {
            Some((_, prior)) => *prior = (*prior).min(l.prior_amount),
            None => per_ledger.push((l, l.prior_amount)),
        }
    }
    per_ledger
        .into_iter()
        .map(|(l, prior_amount)| LaneWatermark {
            pool_id: l.pool_id,
            signer: l.signer,
            provider: l.provider,
            prior_amount,
            // Taken before the settlement read, so the settlement is at or above it.
            rebase_anchor: l.ledger.take_unsaved_rebase(),
            settlement: l.ledger.settlement(),
        })
        .collect()
}

/// One lane's buyer-store watermark write: the lane and the progress to record.
type LaneWrite = (LaneKey, VoucherProgress);

/// The watermark write every face lane calls for, read from the retained
/// handles now — the same settle-at-armed-cumulative rule the file path
/// applies, one write per lane ledger.
fn face_watermark_writes(handles: &[FaceLaneHandle]) -> Vec<LaneWrite> {
    multi_lane_watermarks(&stream_lane_watermarks(handles))
}

/// Apply every write in `writes`. Blocking: each write that moves the row is a
/// redb commit.
fn write_watermarks(store: &dyn BuyerPoolStore, row: WatermarkRow, writes: Vec<LaneWrite>) {
    for (lane, vprogress) in writes {
        persist_watermark(store, row, lane, &vprogress);
    }
}

/// Read every face lane's voucher watermark from its ledger now
/// ([`face_watermark_writes`]), queue the writes on the command's ordered
/// `writes`, and return a receiver that resolves once they land. The faces do
/// not persist, so the caller does it after the fetch, before surfacing
/// any error: the bytes each lane delivered are paid for whatever the outcome.
///
/// Each write that moves the row is a durable redb commit, and a `bundle
/// pull`'s lanes share the caller's task, so a commit inline would stop every
/// lane of the run while it syncs (#2211). The queue keeps the writes in the
/// order the ledgers were read, across every entry: a rebase replaces the lane
/// row outright, so a rebase landing after a newer advance, or a stale advance
/// landing after a rebase, would leave the next run signing from the wrong
/// progress. The read and the queueing are one synchronous call for that
/// reason, and the order holds only while every caller polls on one task, as
/// a fetch and a bundle pull do.
///
/// A drop guard, which cannot wait, drops the receiver. The command then waits
/// for the queue before it returns ([`OrderedWrites::settle`]), on a Ctrl-C
/// too. A second Ctrl-C exits the process at once, and a write still queued or
/// running then is lost; the next run may re-sign a stale watermark, which its
/// provider rejects. A runtime that shuts down cancels blocking tasks that have
/// not started, and the queue then drains on the shutting-down thread.
pub(crate) fn queue_face_watermarks(
    writes: &OrderedWrites,
    store: &Arc<dyn BuyerPoolStore>,
    row: WatermarkRow,
    handles: &[FaceLaneHandle],
) -> tokio::sync::oneshot::Receiver<()> {
    queue_watermark_writes(writes, store, row, face_watermark_writes(handles))
}

/// Queue `lane_writes` on `writes` ([`queue_face_watermarks`]).
fn queue_watermark_writes(
    writes: &OrderedWrites,
    store: &Arc<dyn BuyerPoolStore>,
    row: WatermarkRow,
    lane_writes: Vec<LaneWrite>,
) -> tokio::sync::oneshot::Receiver<()> {
    let store = Arc::clone(store);
    writes.queue_awaitable(move || write_watermarks(store.as_ref(), row, lane_writes))
}

/// One lane's persisted-watermark inputs, lifted out of [`MultiLane`] so the
/// per-lane settlement rule is a pure function the tests can drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneWatermark {
    pool_id: PoolId,
    /// The key the lane's vouchers sign under.
    signer: Address,
    provider: Address,
    /// The lane's persisted prior amount — the baseline the advance is computed
    /// against.
    prior_amount: U256,
    /// The lane's ARMED cumulative: what the lane owes if every voucher it put
    /// on the wire was received.
    settlement: Cumulative,
    /// The watermark the lane's ledger rebased down to and has not persisted.
    rebase_anchor: Option<Cumulative>,
}

/// Pair each lane's own `LaneKey` with the watermark to persist for it.
///
/// Every lane settles at its ARMED cumulative rather than branching on the
/// fetch's outcome. The fetch result is ONE outcome shared by
/// every lane, but "did this lane's last voucher land?" is a PER-LANE question,
/// and on the multi-source path a successful fetch can leave a lane
/// armed-above-committed: a cancelled or stalled leg drops its `fill_gap`
/// future wherever it is parked, including inside the voucher exchange that
/// `issue` deliberately arms before sending, and a leg stopped at a steal
/// split keeps a closing voucher that failed to send armed. Settling that lane at `committed` on the
/// fetch's `Ok` persists a cumulative BELOW what the node can redeem, and the
/// next fetch on that lane signs a cumulative the upstream already holds —
/// rejected as a regression.
///
/// Settling high costs nothing on the clean path: with nothing armed,
/// `settlement()` equals `committed()`.
fn multi_lane_watermarks(lanes: &[LaneWatermark]) -> Vec<(LaneKey, VoucherProgress)> {
    lanes
        .iter()
        .map(|l| {
            (
                LaneKey {
                    pool_id: l.pool_id,
                    signer: l.signer,
                    provider: l.provider,
                },
                VoucherProgress::from_cumulative(l.settlement, l.prior_amount)
                    .with_rebase_anchor(l.rebase_anchor),
            )
        })
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
