//! G-GOV-02 end-to-end: a ratified governance parameter reaches a live daemon
//! (#1041).
//!
//! The parameter under test is `PaymentChannel`'s delivery-rate floor
//! (`getRateBounds()`). The daemon seeds its clamp from an
//! authoritative startup read and then tracks `RateBoundsUpdated`
//! (`crates/node/src/rate_bounds_watcher.rs`, #1172), so a governance retune is
//! supposed to reach a *running* node with no restart and no config edit. This
//! journey asserts exactly that, end to end, through the real Governor:
//!
//! 1. **Baseline.** Chain deploys with a floor of `1`; the node fixture's config
//!    quotes `rate_per_mb = 10`, which is already above it, so the daemon's
//!    `ProbeResponse` advertises the configured 10 verbatim.
//! 2. **Vote weight.** Age the chain past the ~180-day ramp and have the
//!    operator serve real bytes, so it carries nonzero Governor weight
//!    (ADR-036 served bytes × age ramp) at the proposal snapshot.
//! 3. **Ratify.** propose → warp `votingDelay` → `castVote(For)` → warp
//!    `votingPeriod` → `queue`, with the executed action
//!    `PaymentChannel.setRateBounds(50)`.
//! 4. **Negative — pre-timelock.** With the proposal queued but the Timelock
//!    delay not yet elapsed, chain state *and* the live daemon must still be on
//!    the old floor: `getRateBounds()` is `1` and the probe still quotes 10,
//!    held stably across several watcher ticks.
//! 5. **Happy path.** Warp the Timelock delay, `execute`, and assert the live
//!    daemon reprices: with no restart and no config change its advertised
//!    `rate_per_mb` moves 10 → 50, the new floor. That is the whole point — the
//!    quote is now governed by a value that arrived over the wire, not config.
//! 6. **Paid path.** The reprice must also govern what the daemon *sells* at,
//!    not just what it advertises. A fresh paid fetch settles on-chain, and the
//!    settled voucher is read back to confirm the blob was sold at the new floor
//!    — settling alone proves little, since the on-chain floor check rejects
//!    only *under*-payment, so an over-priced sale would settle just as cleanly.
//! 7. **Negative — out-of-safety-bounds.** `setRateBounds` reverts with
//!    `RateBoundsInvalid` for `floor < MIN_DEPOSIT_FLOOR` and for a floor above
//!    `MAX_RATE_PER_MB`, the ADR 005 wire cap the schema will carry — proven as
//!    `from = Timelock` static calls for both (so the
//!    `GOVERNANCE_ROLE` gate is passed and the bounds check is the only thing
//!    that can reject), and — for the sub-floor case — as a full ratified
//!    proposal whose `execute` reverts with the same selector. The daemon keeps
//!    quoting under the last valid floor.
//! 8. **Release.** Every step above tightens. A last proposal ratifies a floor
//!    of `1`, back below the configured rate, and the live quote must fall back
//!    to the configured 10 — proving the clamp releases as well as binds, and
//!    that a lowered floor is not latched at its previous high-water mark.
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
//! *test name*, so a bare `g_gov_02` matches nothing. Since nextest 0.9.85 that
//! is at least loud — `--no-tests` defaults to `fail`, so it exits 4 rather than
//! reporting a zero-test success.

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
/// `decdn_incentive` binding covers the channel-lifecycle and settlement surface,
/// and carries the two rate-bounds members the daemon needs — `getRateBounds()`
/// and the `RateBoundsUpdated` event its watcher subscribes to — but declares no
/// governance setters and no custom errors at all. The Timelock-executed setter
/// and the error it reverts with are therefore declared here rather than widening
/// the shared fixture bindings — this journey is their only consumer.
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
            /// `newFloor < MIN_DEPOSIT_FLOOR` or `newFloor > type(uint64).max`.
            error RateBoundsInvalid(uint256 deliveryFloor);

            function setRateBounds(uint256 newFloor) external;
        }
    }
}

use gov_abi::PaymentChannelGov;

const MIB: usize = 1024 * 1024;
const DAY: u64 = 24 * 60 * 60;
/// `PaymentChannel.BYTES_PER_MB` — the divisor in the per-byte price floor.
const BYTES_PER_MB: u64 = 1_048_576;

/// The floor the deploy script ships (`BaseProtocolDeploy.PAYMENT_DELIVERY_FLOOR`).
const OLD_FLOOR: u64 = 1;
/// The floor this journey ratifies. Deliberately above the fixture's configured
/// `rate_per_mb = 10`, so "the daemon picked it up" is observable as a changed
/// quote rather than as an unchanged one.
const NEW_FLOOR: u64 = 50;
/// A third floor, ratified last, back below the configured rate. Every step
/// before it tightens the clamp; without this leg a watcher that latched the
/// floor at its high-water mark instead of storing the newest value would pass
/// unnoticed.
const RELEASED_FLOOR: u64 = 1;
/// The ADR 005 wire cap, mirrored on-chain as `PaymentChannel.MAX_RATE_PER_MB`.
const MAX_RATE_PER_MB: u64 = 1_000_000_000_000;
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

    // ---- Baseline: chain ships a floor of `1`, and the daemon quotes its
    // configured rate verbatim because it already sits above that floor.
    assert_eq!(
        read_rate_bounds(&chain).await?,
        U256::from(OLD_FLOOR),
        "the deploy script's launch floor must be the starting point"
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
    let outcome = client
        .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
        .await?;
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

    // ---- Ratify the new floor: propose → vote → queue. Stops short of
    // `execute` so the pre-timelock negative below is a real observation.
    let mut retune = Proposal::new(
        payment_channel,
        set_rate_bounds_calldata(U256::from(NEW_FLOOR)),
        format!("retune the delivery rate floor to {NEW_FLOOR}"),
    );
    // `propose_and_queue` asserts the proposal reached `Queued` itself.
    retune.propose_and_queue(&chain, node.operator()).await?;

    // ---- Negative: pre-timelock, the OLD floor still applies. The proposal is
    // queued but the Timelock delay has not elapsed, so neither chain state nor
    // the live daemon may have moved. Held across several watcher ticks so this
    // is a stability claim, not a single lucky read that merely beat the
    // watcher: the fixture sets `event_poll_interval_ms = 500`, but the
    // rate-bounds watcher floors its cadence at 1s
    // (`rate_bounds_watcher.rs`, `event_poll_interval.max(1s)`), so the ~3s this
    // loop spans covers roughly three ticks rather than six.
    assert_eq!(
        read_rate_bounds(&chain).await?,
        U256::from(OLD_FLOOR),
        "queueing alone must not move the on-chain floor"
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
        U256::from(NEW_FLOOR),
        "the executed proposal must have written the new floor"
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

    // ---- The retune must reach the *sell* path too, not just the probe quote.
    // The clamp has separate call sites for the `ProbeResponse`
    // (`handlers/probe.rs`) and the signed `StreamResponse` a buyer actually acts
    // on (`handlers/client/wire.rs`), so observing only the probe would miss a
    // regression where the two disagree.
    //
    // Settling is necessary but NOT sufficient to prove that: the on-chain floor
    // check (`PaymentChannel._advanceClaimWatermark` → `RateFloorViolation`) is
    // one-sided — it rejects paying too *little*, so a node that sold above the
    // floor would settle perfectly cleanly. The price itself is therefore the
    // observable, read back off the channel the vouchers were signed against.
    //
    // Note the under-pricing direction never reaches the chain at all: the node
    // applies the same floor check at zero tolerance before countersigning
    // (`handlers/client/voucher.rs`), so a stale-rate voucher fails the fetch
    // outright rather than settling short. That guard is not observable from an
    // honest client, which is why this leg asserts on price rather than trying to
    // provoke `RateFloorViolation`.
    let served_before = chain.served_bytes(node.operator_addr()).await?;
    let paid = client
        .fetch(&chain, &node, hash, alloy::primitives::U256::ZERO)
        .await?;
    assert_eq!(
        paid.bytes, payload,
        "the node must still deliver the blob under the ratified floor"
    );
    let advanced = poll(Duration::from_secs(120), || async {
        let b = chain.served_bytes(node.operator_addr()).await?;
        Ok((b > served_before).then_some(b))
    })
    .await?;
    assert!(
        advanced.is_some(),
        "a delivery paid at the ratified floor must settle on-chain — served-bytes never advanced"
    );

    // The rate the blob was actually sold at, recovered from the settled voucher.
    // Measured: 100 micro-USDC against 2 MiB of claimed bytes, i.e. exactly the
    // floor. That also means the settlement sits exactly on the contract's own
    // limit — `maxBytes = mulDiv(100, BYTES_PER_MB, 50) == bytesDelivered` — which
    // `_advanceClaimWatermark` admits only because it rejects on `>` rather than
    // `>=`. Worth knowing: there is no headroom on the under-payment side, so a
    // future change to voucher pricing or blob size will surface here first.
    //
    // The `+1` tolerance is for the over-payment side only: each voucher interval
    // prices its delta with `div_ceil`, so a different interval split could round
    // a micro-USDC up. The regression this leg exists to catch — selling at the
    // unclamped configured rate — is nowhere near the tolerance.
    let (amount, billed_bytes) = settled_amount_and_bytes(&chain, paid.channel_id).await?;
    assert!(
        billed_bytes >= U256::from(2 * MIB),
        "the settled voucher must cover the whole blob, got {billed_bytes} billed bytes"
    );
    let implied_rate = amount * U256::from(BYTES_PER_MB) / billed_bytes;
    assert!(
        (U256::from(NEW_FLOOR)..=U256::from(NEW_FLOOR + 1)).contains(&implied_rate),
        "the blob must be sold at the ratified floor ({NEW_FLOOR}/MB): settled {amount} \
         micro-USDC for {billed_bytes} billed bytes = {implied_rate}/MB. A sale at the unclamped \
         configured rate ({CONFIGURED_RATE}/MB) would be rejected, but any price *above* the \
         floor would settle just as cleanly on-chain, which is why this asserts the price and \
         not merely that settlement happened."
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
    simulate_set_rate_bounds(&chain, timelock, U256::from(2))
        .await
        .context("an in-bounds floor must simulate cleanly from the Timelock")?;
    expect_revert::<_, PaymentChannelGov::RateBoundsInvalid>(
        simulate_set_rate_bounds(&chain, timelock, U256::ZERO).await,
        "setRateBounds below MIN_DEPOSIT_FLOOR",
    )?;
    // The floor is capped at the ADR 005 wire constant `MAX_RATE_PER_MB`, not at
    // `type(uint64).max`. Every value in the gap between them is quietly
    // network-isolating: nodes raise every quote to the floor before signing, so
    // such a floor makes every response undecodable to every honest peer while
    // looking locally like a routine clamp. Test both ends of that gap — one
    // above the wire cap, and `u64::MAX` itself, which an earlier guard allowed.
    expect_revert::<_, PaymentChannelGov::RateBoundsInvalid>(
        simulate_set_rate_bounds(
            &chain,
            timelock,
            U256::from(MAX_RATE_PER_MB) + U256::from(1),
        )
        .await,
        "setRateBounds above the MAX_RATE_PER_MB wire cap",
    )?;
    expect_revert::<_, PaymentChannelGov::RateBoundsInvalid>(
        simulate_set_rate_bounds(&chain, timelock, U256::from(u64::MAX)).await,
        "setRateBounds at the u64 cap, far above the wire cap",
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
        set_rate_bounds_calldata(U256::ZERO),
        "retune the delivery rate floor below the safety floor".to_owned(),
    );
    // `propose_and_queue` pins the *Governor* state at `Queued`, so "never
    // reached Queued" (lost quorum, a bad warp) is already distinguishable from
    // the safety-bounds rejection under test. That says nothing about Timelock
    // readiness — a separate state machine, warped inside the call below.
    bad.propose_and_queue(&chain, node.operator()).await?;
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
        U256::from(NEW_FLOOR),
        "a reverted execution must leave the last ratified floor in place"
    );
    assert_eq!(
        client.probe(&node, hash).await?.body.rate_per_mb,
        NEW_FLOOR,
        "the daemon must keep quoting under the last valid floor"
    );

    // ---- Finally, the release direction. Every step above raised the floor, so
    // a watcher that latched it at its high-water mark — or a clamp that only
    // ever moved a quote up — would have passed every assertion so far.
    // Ratifying a floor back *below* the configured rate is the only leg that
    // separates "the newest value is stored" from "the largest value is stored":
    // the quote must fall back to the configured rate the clamp no longer binds.
    let mut release = Proposal::new(
        payment_channel,
        set_rate_bounds_calldata(U256::from(RELEASED_FLOOR)),
        format!("retune the delivery rate floor to {RELEASED_FLOOR}"),
    );
    release.propose_and_queue(&chain, node.operator()).await?;
    release
        .execute_after_timelock(&chain, node.operator())
        .await?;
    assert_eq!(
        read_rate_bounds(&chain).await?,
        U256::from(RELEASED_FLOOR),
        "the releasing proposal must have written the third floor"
    );
    let released = poll(Duration::from_secs(60), || async {
        let quote = client.probe(&node, hash).await?.body.rate_per_mb;
        Ok((quote != NEW_FLOOR).then_some(quote))
    })
    .await?;
    assert_eq!(
        released,
        Some(CONFIGURED_RATE),
        "with the ratified floor ({RELEASED_FLOOR}) back below the configured rate, the live \
         daemon must quote the configured {CONFIGURED_RATE} again — proving the clamp releases \
         and does not latch at its high-water mark"
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
        )?;

        // Every caller needs this, so assert it once here rather than at each
        // site: reaching `Queued` is what proves quorum was met and the timelock
        // op was scheduled, and it localizes a lost-vote-weight failure to the
        // proposal that lost it instead of to whatever runs next.
        let state = self.state(chain).await?;
        anyhow::ensure!(
            state == PROPOSAL_STATE_QUEUED,
            "proposal must reach Queued after queue() (quorum met, timelock scheduled), got \
             state {state}"
        );
        Ok(())
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

    /// Warp past the Timelock `minDelay` and `execute`.
    ///
    /// The split is by *what the caller can conclude*, not by where the failure
    /// physically happened: the outer `Result` means this fixture could not put
    /// the question to the chain (RPC down, bad warp, or a rejection carrying no
    /// revert payload to match), while the inner one means the chain rejected it
    /// and said why in decodable form.
    ///
    /// The inner error is the typed `alloy::contract::Error` rather than a string
    /// so callers can selector-match it through `expect_revert`. Note that the
    /// routing alone does not identify the guard under test — a missing role or
    /// an unready Timelock also revert *with* data and so also land in the inner
    /// arm. It is `expect_revert`'s selector check that separates those from
    /// `RateBoundsInvalid`; the routing's job is only to keep payload-less
    /// failures from ever reaching that check.
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

/// ABI-encoded `PaymentChannel.setRateBounds(floor)`.
fn set_rate_bounds_calldata(floor: U256) -> Bytes {
    PaymentChannelGov::setRateBoundsCall { newFloor: floor }
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

/// The settled `(claimedAmount, claimedBytes)` watermark for `channel_id` — what
/// the node actually charged, in micro-USDC against bao wire bytes. Read after a
/// paid fetch so the leg above can assert on price rather than on the mere fact
/// that settlement succeeded.
async fn settled_amount_and_bytes(
    chain: &ChainFixture,
    channel_id: B256,
) -> anyhow::Result<(U256, U256)> {
    let ch = PaymentChannel::new(chain.addrs().payment_channel, chain.admin())
        .getChannel(channel_id)
        .call()
        .await
        .context("getChannel")?;
    Ok((ch.claimedAmount, ch.claimedBytes))
}

async fn read_rate_bounds(chain: &ChainFixture) -> anyhow::Result<U256> {
    PaymentChannel::new(chain.addrs().payment_channel, chain.admin())
        .getRateBounds()
        .call()
        .await
        .context("getRateBounds")
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
    floor: U256,
) -> Result<(), alloy::contract::Error> {
    PaymentChannelGov::new(chain.addrs().payment_channel, chain.admin())
        .setRateBounds(floor)
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
