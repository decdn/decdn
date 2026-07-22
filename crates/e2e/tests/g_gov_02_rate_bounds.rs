//! G-GOV-02 end-to-end: a ratified governance parameter reaches a live daemon
//! (#1041).
//!
//! The parameter under test is `PaymentChannel`'s delivery-rate band
//! (`getRateBounds()` → `[floor, ceiling]`). The daemon seeds its clamp from an
//! authoritative startup read and then tracks `RateBoundsUpdated`
//! (`crates/node/src/rate_bounds_watcher.rs`, #1172), so a governance retune is
//! supposed to reach a *running* node with no restart and no config edit. This
//! journey asserts exactly that, end to end, through the real Governor:
//!
//! 1. **Baseline.** Chain deploys with `[1, 1000]`; the node fixture's config
//!    quotes `rate_per_mb = 10`, which is inside the band, so the daemon's
//!    `ProbeResponse` advertises the configured 10 verbatim.
//! 2. **Vote weight.** Age the chain past the ~180-day ramp and have the
//!    operator serve real bytes, so it carries nonzero Governor weight
//!    (ADR-036 served bytes × age ramp) at the proposal snapshot.
//! 3. **Ratify.** propose → warp `votingDelay` → `castVote(For)` → warp
//!    `votingPeriod` → `queue`, with the executed action
//!    `PaymentChannel.setRateBounds(50, 500)`.
//! 4. **Negative — pre-timelock.** With the proposal queued but the Timelock
//!    delay not yet elapsed, chain state *and* the live daemon must still be on
//!    the old band: `getRateBounds()` is `[1, 1000]` and the probe still quotes
//!    10, held stably across several watcher ticks.
//! 5. **Happy path.** Warp the Timelock delay, `execute`, and assert the live
//!    daemon reprices: with no restart and no config change its advertised
//!    `rate_per_mb` moves 10 → 50, the new floor. That is the whole point — the
//!    quote is now outside the *old* band and inside the new one.
//! 6. **Paid path.** The reprice must also govern what the daemon *sells* at,
//!    not just what it advertises: a fresh paid fetch settles on-chain at the
//!    new floor. A voucher still priced at the stale 10 would be rejected by
//!    `RateFloorViolation` and never advance served-bytes.
//! 7. **Negative — out-of-safety-bounds.** `setRateBounds` reverts with
//!    `RateBoundsInvalid` for `floor < MIN_DEPOSIT_FLOOR` and for
//!    `ceiling <= floor`, proven as `from = Timelock` static calls for both (so
//!    the `GOVERNANCE_ROLE` gate is passed and the bounds check is the only
//!    thing that can reject), and — for the sub-floor case — as a full ratified
//!    proposal whose `execute` reverts with the same selector. The daemon keeps
//!    quoting under the last valid band.
//! 8. **Ceiling.** Every step above moves the floor, so a last proposal ratifies
//!    `[1, 5]`, a band whose *ceiling* is below the configured rate. The live
//!    quote must clamp the other direction, 50 → 5, which only holds if the
//!    event's ceiling field reached the clamp alongside its floor.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a built `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(g_gov_02_rate_bounds)'
//! ```
//!
//! Select the binary, not the file stem: nextest's positional filter matches the
//! *test name*, so a bare `g_gov_02` matches nothing and exits 0 having run zero
//! tests.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::{SolCall, SolError};
use anyhow::Context;
use decdn_e2e::bindings::{DecdnGovernor, PaymentChannel, TimelockController};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::time;

/// `PaymentChannel`'s governance write + safety-bound error. The production
/// `decdn_incentive` binding covers the channel-lifecycle and settlement surface
/// but declares no governance setters and no custom errors — `getRateBounds()`
/// is the only rate-bounds member it carries. The Timelock-executed setter and
/// the error it reverts with are declared here rather than widening the shared
/// fixture bindings — this journey is their only consumer.
mod gov_abi {
    #![allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_used,
        clippy::pub_underscore_fields,
        clippy::missing_docs_in_private_items,
        missing_debug_implementations,
        missing_docs,
        non_camel_case_types,
        non_snake_case
    )]

    alloy::sol! {
        #[sol(rpc)]
        contract PaymentChannelGov {
            /// Thrown by `setRateBounds` (and the constructor) when
            /// `newFloor < MIN_DEPOSIT_FLOOR` or `newCeiling <= newFloor`.
            error RateBoundsInvalid(uint256 deliveryFloor, uint256 deliveryCeiling);

            function setRateBounds(uint256 newFloor, uint256 newCeiling) external;
        }
    }
}

use gov_abi::PaymentChannelGov;

const MIB: usize = 1024 * 1024;
const DAY: u64 = 24 * 60 * 60;

/// The band the deploy script ships (`BaseProtocolDeploy.PAYMENT_DELIVERY_*`).
const OLD_FLOOR: u64 = 1;
const OLD_CEILING: u64 = 1000;
/// The band this journey ratifies. The floor is deliberately above the fixture's
/// configured `rate_per_mb = 10`, so "the daemon picked it up" is observable as a
/// changed quote rather than as an unchanged one.
const NEW_FLOOR: u64 = 50;
const NEW_CEILING: u64 = 500;
/// A third band, ratified last, whose *ceiling* sits below the configured rate
/// so the daemon must clamp **down**. Without this leg every daemon-side
/// observation in the journey is a floor clamp, and a watcher that decoded
/// `newDeliveryFloor` correctly while dropping `newDeliveryCeiling` would pass
/// unnoticed — the two fields have to move as one value.
const TIGHT_FLOOR: u64 = 1;
const TIGHT_CEILING: u64 = 5;
/// `NodeFixture::render_config`'s `[payment] rate_per_mb`.
const CONFIGURED_RATE: u64 = 10;

/// OZ `IGovernor.ProposalState::Queued`.
const PROPOSAL_STATE_QUEUED: u8 = 5;
/// OZ `IGovernor.ProposalState::Executed`.
const PROPOSAL_STATE_EXECUTED: u8 = 7;

/// Generous overall ceiling: three full Governor lifecycles (propose → vote →
/// queue → timelock → execute) plus two real paid deliveries and a daemon
/// subprocess. The chain-time warping is `evm_increaseTime`, which costs no wall
/// clock, so this is a backstop rather than a tight bound.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(1200);

#[tokio::test(flavor = "multi_thread")]
async fn ratified_rate_bounds_reach_a_live_daemon() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-GOV-02 exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payment_channel = chain.addrs().payment_channel;

    // ---- One bonded operator, daemon running. It is both the node under test
    // (its live probe quote is the observable) and the voter — nothing here
    // slashes it, so unlike G-NODE-05 no second operator is needed to carry the
    // vote.
    let payload = vec![0x9Au8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;

    // ---- Baseline: chain ships `[1, 1000]`, and the daemon quotes its
    // configured rate verbatim because it already sits inside that band.
    assert_eq!(
        read_rate_bounds(&chain).await?,
        (U256::from(OLD_FLOOR), U256::from(OLD_CEILING)),
        "the deploy script's launch band must be the starting point"
    );
    let client = ClientFixture::new(&chain).await?;
    assert_eq!(
        client.probe(&node, hash).await?.body.rate_per_mb,
        CONFIGURED_RATE,
        "an unclamped quote must be the configured rate_per_mb"
    );

    // ---- Vote weight (ADR-036 served bytes × age ramp): age past the ~180-day
    // ramp, serve real bytes, then cross an epoch boundary so those bytes sit in
    // a fully-elapsed epoch inside the trailing window at the proposal snapshot.
    // The delivery is settled *before* the retune on purpose: raising the
    // delivery floor tightens `PaymentChannel`'s settlement-side
    // `RateFloorViolation` check, so a voucher priced at the old rate must land
    // on-chain while the old floor is still in force.
    time::increase_time(chain.admin(), 185 * DAY).await?;
    let outcome = client.fetch(&chain, &node, hash).await?;
    assert_eq!(outcome.bytes, payload, "the node must deliver the blob");
    let served = poll(Duration::from_secs(120), || async {
        let b = chain.served_bytes(node.operator_addr()).await?;
        Ok((b > U256::ZERO).then_some(b))
    })
    .await?;
    assert!(
        served.is_some(),
        "the operator's on-chain served-bytes never advanced, so it would carry no vote weight"
    );
    time::increase_time(chain.admin(), 8 * DAY).await?;

    // ---- Ratify the new band: propose → vote → queue. Stops short of
    // `execute` so the pre-timelock negative below is a real observation.
    let mut retune = Proposal::new(
        payment_channel,
        set_rate_bounds_calldata(NEW_FLOOR, NEW_CEILING),
        format!("retune delivery rate bounds to [{NEW_FLOOR}, {NEW_CEILING}]"),
    );
    retune.propose_and_queue(&chain, node.operator()).await?;
    assert_eq!(
        retune.state(&chain).await?,
        PROPOSAL_STATE_QUEUED,
        "the retune must reach Queued (quorum met, timelock scheduled)"
    );

    // ---- Negative: pre-timelock, the OLD bounds still apply. The proposal is
    // queued but the Timelock delay has not elapsed, so neither chain state nor
    // the live daemon may have moved. Held across several watcher ticks so this
    // is a stability claim, not a single lucky read that merely beat the
    // watcher: the fixture sets `event_poll_interval_ms = 500`, but the
    // rate-bounds watcher floors its cadence at 1s
    // (`rate_bounds_watcher.rs`, `event_poll_interval.max(1s)`), so the ~3s this
    // loop spans covers roughly three ticks rather than six.
    assert_eq!(
        read_rate_bounds(&chain).await?,
        (U256::from(OLD_FLOOR), U256::from(OLD_CEILING)),
        "queueing alone must not move the on-chain band"
    );
    for _ in 0..6u32 {
        assert_eq!(
            client.probe(&node, hash).await?.body.rate_per_mb,
            CONFIGURED_RATE,
            "a queued-but-unexecuted proposal must not reprice the live daemon"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ---- Happy path: warp the Timelock delay, execute, and watch the running
    // daemon reprice. Nothing restarts it and nothing edits its config — the
    // only input that changed is chain state.
    retune
        .execute_after_timelock(&chain, node.operator())
        .await?;
    assert_eq!(
        retune.state(&chain).await?,
        PROPOSAL_STATE_EXECUTED,
        "the retune proposal must be Executed"
    );
    assert_eq!(
        read_rate_bounds(&chain).await?,
        (U256::from(NEW_FLOOR), U256::from(NEW_CEILING)),
        "the executed proposal must have written the new band"
    );
    let repriced = poll(Duration::from_secs(60), || async {
        let quote = client.probe(&node, hash).await?.body.rate_per_mb;
        Ok((quote != CONFIGURED_RATE).then_some(quote))
    })
    .await?;
    assert_eq!(
        repriced,
        Some(NEW_FLOOR),
        "the live daemon must clamp its configured rate ({CONFIGURED_RATE}) up to the ratified \
         floor ({NEW_FLOOR}) — outside the old band, inside the new one"
    );

    // ---- The retune must reach the *paid* path too, not just the probe quote.
    // The daemon clamps in three independent places — the `ProbeResponse`, the
    // signed `StreamResponse` a buyer actually acts on, and voucher settlement —
    // so observing only the probe would miss a regression that advertises 50 and
    // then sells at the stale 10. Settlement is the discriminator: a voucher
    // priced at 10 for a 2 MiB blob yields `maxBytes = 20 * 1048576 / 50`, far
    // below the bytes delivered, so `PaymentChannel` would reject it with
    // `RateFloorViolation` and the operator's served-bytes would never advance.
    // Paying the honest 50 lands exactly on the boundary (`maxBytes ==
    // bytesDelivered`), which the contract admits because it rejects only `>`.
    let served_before = chain.served_bytes(node.operator_addr()).await?;
    let paid = client.fetch(&chain, &node, hash).await?;
    assert_eq!(
        paid.bytes, payload,
        "the node must still deliver the blob under the ratified band"
    );
    let advanced = poll(Duration::from_secs(120), || async {
        let b = chain.served_bytes(node.operator_addr()).await?;
        Ok((b > served_before).then_some(b))
    })
    .await?;
    assert!(
        advanced.is_some(),
        "a delivery paid at the ratified floor must settle on-chain — served-bytes never advanced, \
         which is what a voucher still priced at the stale {CONFIGURED_RATE} would look like"
    );

    // Cross an epoch so the bytes just served sit in a fully-elapsed epoch, and
    // therefore carry vote weight at the snapshots of the two proposals below.
    time::increase_time(chain.admin(), 8 * DAY).await?;

    // ---- Negative: out-of-safety-bounds values revert at the contract. Static
    // calls with `from = Timelock` clear the `GOVERNANCE_ROLE` gate, so the
    // bounds check is the only thing that can reject them. The control case
    // proves that framing: an in-bounds pair simulates cleanly from the same
    // sender, so the two rejections below are about the values, not the caller.
    let timelock = chain.addrs().timelock;
    simulate_set_rate_bounds(&chain, timelock, 2, 3)
        .await
        .map_err(|e| {
            anyhow::anyhow!("an in-bounds pair must simulate cleanly from the Timelock: {e}")
        })?;
    expect_revert::<_, PaymentChannelGov::RateBoundsInvalid>(
        simulate_set_rate_bounds(&chain, timelock, 0, 5).await,
        "setRateBounds below MIN_DEPOSIT_FLOOR",
    )?;
    expect_revert::<_, PaymentChannelGov::RateBoundsInvalid>(
        simulate_set_rate_bounds(&chain, timelock, 100, 100).await,
        "setRateBounds with ceiling <= floor",
    )?;

    // ---- ...and the Governor cannot smuggle one past that check either: a
    // fully ratified out-of-bounds proposal reverts at `execute`, with
    // `RateBoundsInvalid` reaching the caller intact. The revert originates two
    // frames down (Governor → Timelock → PaymentChannel), but OZ 5.1.0's
    // `TimelockController._execute` re-reverts through `Address.verifyCallResult`,
    // which bubbles the raw returndata verbatim — so the selector survives and
    // this negative can be held to the same standard as the static calls above.
    // Matching it is what stops an unready Timelock or a transport fault from
    // passing here: every assertion below is equally satisfied by "the
    // transaction never landed", so `is_err()` alone would prove nothing.
    let mut bad = Proposal::new(
        payment_channel,
        set_rate_bounds_calldata(0, 5),
        "retune delivery rate bounds below the safety floor".to_owned(),
    );
    bad.propose_and_queue(&chain, node.operator()).await?;
    // Pin readiness *before* executing, so a failure to reach `Ready` is
    // distinguishable from the safety-bounds rejection under test.
    assert_eq!(
        bad.state(&chain).await?,
        PROPOSAL_STATE_QUEUED,
        "the out-of-bounds proposal must reach Queued before its execute is judged"
    );
    expect_revert::<_, PaymentChannelGov::RateBoundsInvalid>(
        bad.try_execute_after_timelock(&chain, node.operator())
            .await?,
        "executing an out-of-safety-bounds proposal",
    )?;
    assert_eq!(
        bad.state(&chain).await?,
        PROPOSAL_STATE_QUEUED,
        "a reverted execution must leave the proposal unexecuted"
    );
    assert_eq!(
        read_rate_bounds(&chain).await?,
        (U256::from(NEW_FLOOR), U256::from(NEW_CEILING)),
        "a reverted execution must leave the last ratified band in place"
    );
    assert_eq!(
        client.probe(&node, hash).await?.body.rate_per_mb,
        NEW_FLOOR,
        "the daemon must keep quoting under the last valid band"
    );

    // ---- Finally, the ceiling half of the band. Everything above moved the
    // *floor*, so a watcher that decoded `newDeliveryFloor` and dropped
    // `newDeliveryCeiling` — storing `(50, u64::MAX)` — would have passed every
    // assertion so far. Ratifying a band whose ceiling sits below the configured
    // rate forces the opposite clamp direction: the quote must come *down* to
    // the ceiling, which is only observable if both fields survived the event.
    let mut tighten = Proposal::new(
        payment_channel,
        set_rate_bounds_calldata(TIGHT_FLOOR, TIGHT_CEILING),
        format!("retune delivery rate bounds to [{TIGHT_FLOOR}, {TIGHT_CEILING}]"),
    );
    tighten.propose_and_queue(&chain, node.operator()).await?;
    tighten
        .execute_after_timelock(&chain, node.operator())
        .await?;
    assert_eq!(
        read_rate_bounds(&chain).await?,
        (U256::from(TIGHT_FLOOR), U256::from(TIGHT_CEILING)),
        "the tightening proposal must have written the third band"
    );
    let clamped_down = poll(Duration::from_secs(60), || async {
        let quote = client.probe(&node, hash).await?.body.rate_per_mb;
        Ok((quote != NEW_FLOOR).then_some(quote))
    })
    .await?;
    assert_eq!(
        clamped_down,
        Some(TIGHT_CEILING),
        "the live daemon must clamp its configured rate ({CONFIGURED_RATE}) down to the ratified \
         ceiling ({TIGHT_CEILING}) — proving the event's ceiling field reached the clamp too"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Governor plumbing
// ---------------------------------------------------------------------------

/// A single-action Governor proposal, split at `queue` so a caller can assert
/// on the pre-timelock world before executing.
///
/// Lives in the test rather than on `ChainFixture` because it is the only
/// consumer: `governor_grant_appeal` there is the same lifecycle fused into one
/// call, which cannot express the pre-timelock observation this journey needs.
struct Proposal {
    targets: Vec<Address>,
    values: Vec<U256>,
    calldatas: Vec<Bytes>,
    description: String,
    desc_hash: B256,
    /// Filled by `propose_and_queue`; the deterministic `hashProposal` id.
    id: U256,
}

impl Proposal {
    fn new(target: Address, calldata: Bytes, description: String) -> Self {
        let desc_hash = keccak256(description.as_bytes());
        Self {
            targets: vec![target],
            values: vec![U256::ZERO],
            calldatas: vec![calldata],
            description,
            desc_hash,
            id: U256::ZERO,
        }
    }

    /// propose → warp `votingDelay` → `castVote(For)` → warp `votingPeriod` →
    /// `queue`. `voter` must already carry nonzero vote weight at the snapshot;
    /// it is also the proposer (it must clear `proposalThreshold`).
    async fn propose_and_queue(
        &mut self,
        chain: &ChainFixture,
        voter: &PrivateKeySigner,
    ) -> anyhow::Result<()> {
        chain.fund_eth(voter.address(), 10).await?;
        let provider = chain.provider_for(voter);
        let gov = DecdnGovernor::new(chain.addrs().governor, &provider);

        // Static call returns the proposalId the send will mint (deterministic
        // `hashProposal`); trust it only after the send is mined.
        let proposal_id = gov
            .propose(
                self.targets.clone(),
                self.values.clone(),
                self.calldatas.clone(),
                self.description.clone(),
            )
            .call()
            .await
            .context("propose static call")?;
        mined(
            gov.propose(
                self.targets.clone(),
                self.values.clone(),
                self.calldatas.clone(),
                self.description.clone(),
            )
            .send()
            .await
            .context("propose send")?
            .get_receipt()
            .await
            .context("propose receipt")?
            .status(),
            "propose",
        )?;
        self.id = proposal_id;

        let voting_delay = gov.votingDelay().call().await.context("read votingDelay")?;
        time::increase_time(chain.admin(), voting_delay.to::<u64>() + 2).await?;

        mined(
            gov.castVote(proposal_id, 1) // 1 = For
                .send()
                .await
                .context("castVote send")?
                .get_receipt()
                .await
                .context("castVote receipt")?
                .status(),
            "castVote",
        )?;

        let voting_period = gov
            .votingPeriod()
            .call()
            .await
            .context("read votingPeriod")?;
        time::increase_time(chain.admin(), voting_period.to::<u64>() + 2).await?;

        mined(
            gov.queue(
                self.targets.clone(),
                self.values.clone(),
                self.calldatas.clone(),
                self.desc_hash,
            )
            .send()
            .await
            .context("queue send")?
            .get_receipt()
            .await
            .context("queue receipt")?
            .status(),
            "queue",
        )
    }

    /// Warp past the Timelock `minDelay` and `execute`, requiring success.
    async fn execute_after_timelock(
        &self,
        chain: &ChainFixture,
        voter: &PrivateKeySigner,
    ) -> anyhow::Result<()> {
        self.try_execute_after_timelock(chain, voter)
            .await?
            .map_err(|e| anyhow::anyhow!("execute reverted: {e}"))
    }

    /// Warp past the Timelock `minDelay` and `execute`. The outer `Result` is a
    /// harness failure (RPC, missing role, bad warp); the inner one is the
    /// on-chain outcome, so the out-of-bounds negative can assert on a revert
    /// without conflating it with a broken fixture.
    ///
    /// The inner error stays the typed `alloy::contract::Error` rather than a
    /// string so callers can selector-match it through `expect_revert`. That is
    /// what keeps the out-of-bounds negative honest: only a rejection that
    /// actually carries revert data reaches the inner arm, so a transport fault
    /// or an unready Timelock can no longer masquerade as the safety-bounds
    /// guard firing.
    async fn try_execute_after_timelock(
        &self,
        chain: &ChainFixture,
        voter: &PrivateKeySigner,
    ) -> anyhow::Result<Result<(), alloy::contract::Error>> {
        let provider = chain.provider_for(voter);
        let min_delay = TimelockController::new(chain.addrs().timelock, &provider)
            .getMinDelay()
            .call()
            .await
            .context("read timelock minDelay")?;
        time::increase_time(chain.admin(), min_delay.to::<u64>() + 2).await?;

        let gov = DecdnGovernor::new(chain.addrs().governor, &provider);
        // A reverting inner call usually surfaces at gas estimation, so `send()`
        // itself errors and never reaches a receipt; the mined-but-reverted case
        // is handled below because alloy resolves that as `Ok`.
        let pending = match gov
            .execute(
                self.targets.clone(),
                self.values.clone(),
                self.calldatas.clone(),
                self.desc_hash,
            )
            .send()
            .await
        {
            Ok(pending) => pending,
            // Only a rejection carrying revert data is an on-chain outcome. A
            // transport fault, a nonce collision, or an unfunded sender has no
            // revert payload and is a broken harness, so it propagates outward
            // instead of being reported as "the contract rejected it".
            Err(e) if e.as_revert_data().is_some() => return Ok(Err(e)),
            Err(e) => {
                return Err(
                    anyhow::Error::new(e).context("execute send failed with no revert data")
                );
            }
        };
        let receipt = pending.get_receipt().await.context("execute receipt")?;
        if receipt.status() {
            Ok(Ok(()))
        } else {
            // Mined-but-reverted carries no payload for alloy to decode, so it
            // cannot be selector-matched. Surface it as a harness failure with
            // the tx hash rather than letting an unmatchable revert stand in for
            // the guard under test.
            Err(anyhow::anyhow!(
                "execute mined but reverted (tx {:#x}); no revert payload to match",
                receipt.transaction_hash
            ))
        }
    }

    async fn state(&self, chain: &ChainFixture) -> anyhow::Result<u8> {
        DecdnGovernor::new(chain.addrs().governor, chain.admin())
            .state(self.id)
            .call()
            .await
            .context("read proposal state")
    }
}

/// ABI-encoded `PaymentChannel.setRateBounds(floor, ceiling)`.
fn set_rate_bounds_calldata(floor: u64, ceiling: u64) -> Bytes {
    PaymentChannelGov::setRateBoundsCall {
        newFloor: U256::from(floor),
        newCeiling: U256::from(ceiling),
    }
    .abi_encode()
    .into()
}

/// Bail if a mined transaction reverted (alloy resolves those as `Ok`).
fn mined(status: bool, what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(status, "{what} reverted on-chain");
    Ok(())
}

// ---------------------------------------------------------------------------
// Contract-side assertions
// ---------------------------------------------------------------------------

async fn read_rate_bounds(chain: &ChainFixture) -> anyhow::Result<(U256, U256)> {
    let b = PaymentChannel::new(chain.addrs().payment_channel, chain.admin())
        .getRateBounds()
        .call()
        .await
        .context("getRateBounds")?;
    Ok((b.floor, b.ceiling))
}

/// `eth_call` `setRateBounds` as `caller`. No state is written either way; the
/// point is to reach the contract's safety-bounds check with the role gate
/// already satisfied.
///
/// Returns the raw call result: a transport fault and a contract revert share
/// this one channel, and `expect_revert` is what separates them — it requires
/// revert data to be present, so a dead RPC fails loudly instead of counting as
/// a rejection.
async fn simulate_set_rate_bounds(
    chain: &ChainFixture,
    caller: Address,
    floor: u64,
    ceiling: u64,
) -> Result<(), alloy::contract::Error> {
    PaymentChannelGov::new(chain.addrs().payment_channel, chain.admin())
        .setRateBounds(U256::from(floor), U256::from(ceiling))
        .from(caller)
        .call()
        .await
        .map(|_| ())
}

/// Assert an alloy contract call reverted with exactly `E`, matching on the
/// 4-byte selector. Distinguishes the guard under test from a transport fault
/// (no revert data at all) and from a *different* revert — notably the
/// `AccessControlUnauthorizedAccount` gate, which an `is_err()` check would
/// happily accept and which would make this negative vacuous. Mirrors the same
/// helper in `g_origin_01_publish.rs`.
fn expect_revert<T, E: SolError>(
    result: Result<T, alloy::contract::Error>,
    what: &str,
) -> anyhow::Result<()> {
    let Err(err) = result else {
        anyhow::bail!("{what} must revert with {}, but succeeded", E::SIGNATURE)
    };
    let data = err.as_revert_data().with_context(|| {
        format!(
            "{what}: expected a {} revert, got no revert data: {err}",
            E::SIGNATURE
        )
    })?;
    let selector = data
        .get(..4)
        .context("revert payload too short to carry a selector")?;
    anyhow::ensure!(
        selector == E::SELECTOR,
        "{what}: expected {}, got revert data 0x{}",
        E::SIGNATURE,
        alloy::hex::encode(&data)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

/// Poll `f` every 500ms until it yields `Some` or `timeout` elapses. Returns
/// `None` on timeout so the caller owns the failure message.
async fn poll<T, F, Fut>(timeout: Duration, mut f: F) -> anyhow::Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await? {
            return Ok(Some(v));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
