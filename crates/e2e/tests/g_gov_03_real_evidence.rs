//! G-GOV-03 — real node responses as valid on-chain evidence (issue #1042).
//!
//! Every other "signed evidence" path in this harness cheats: it builds a
//! `ProbeSlashData` / `StreamSlashData` in the test and signs it with
//! `node.operator()`, because the test holds the operator's key. That proves the
//! *contract* accepts a well-formed signature — which `SlashJudge.t.sol` already
//! proves with synthetic ones — and proves nothing about the daemon. This journey
//! closes that gap: every byte of evidence submitted here came off the wire from a
//! running `decdn-node`, `slash_sig` included, and is forwarded verbatim.
//!
//! Two nodes, one offense each, both induced through real operator surfaces:
//!
//! - **Phantom announcement (node A).** A holds `H` and answers a probe with a
//!   signed `has_blob: true`. The operator then evicts `H` over the admin RPC, and
//!   the next paid stream on the *same already-accepted channel* comes back as a
//!   signed `ok: false`. Those two daemon-signed messages, seconds apart, are the
//!   `SlashJudge` phantom pair.
//! - **Rate bait-and-switch (node B).** B answers a probe at its configured rate,
//!   the operator raises `payment.rate_per_mb` and hot-reloads it (no restart —
//!   the field is in the reloadable set), and B's next signed `StreamResponse`
//!   quotes the higher rate inside the 30s window. That is the
//!   `submitRateChallenge` pair.
//!
//! The positive path asserts the full on-chain consequence: the slash lands, the
//! TOKEN is escrowed (ADR 028 escrow-on-slash) rather than distributed
//! immediately, `slashedAtEpoch` is stamped so ADR-036 vote weight zeroes out,
//! and once the 30-day filing window lapses `finalizeUnappealedSlash` splits the
//! escrow 50% to the challenger / 50% burned.
//!
//! Negatives, all built from the *same real* daemon output so they test the
//! judge and not the test's own forgery skills:
//!
//! - a genuine probe stamped 40s before the stream is rejected
//!   (`TimestampWindowViolated`) — the 30s window is enforced on real evidence;
//! - the real probe with its signature swapped for one from an unrelated key is
//!   rejected (`InvalidProbeSignature`);
//! - a failed challenge moves no TOKEN. Note the issue text says the challenger
//!   "forfeits" its bond; the as-built contract does not, and deliberately so —
//!   ADR 014 § Bond Handling: "a `submit*Challenge` that fails on-chain
//!   verification reverts and the challenger pays only gas; the bond is not
//!   transferred for failed verifications." There is no second-stage dispute that
//!   could claw it back after the fact. So the assertion is the behaviour of
//!   record: nothing is pulled, and no slash is minted.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and built `decdn-node` + `decdn` binaries:
//!
//! ```bash
//! cargo build -p decdn-node -p decdn
//! cargo nextest run -p decdn-e2e --features anvil-e2e
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    // `node_a_id` / `node_b_id` and friends: the A/B suffix IS the distinction the
    // journey is about, and renaming to satisfy the heuristic would obscure it.
    clippy::similar_names,
    // The journey is deliberately one linear narrative (capture → negatives →
    // positive → finality) sharing one anvil deployment; splitting it would either
    // duplicate a ~60s deploy per stage or thread the whole world through helpers.
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_common::admin::{AdminRpcClient, EvictRequest};
use decdn_e2e::assert::expect_revert_anyhow;
use decdn_e2e::bindings::SlashJudgeRate;
use decdn_e2e::chain::{ChainFixture, EvidencePair, Offense};
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::time;
use decdn_incentive::{ProbeSlashData, slash_judge_domain};
use decdn_protocol::ProbeResponse;
use decdn_protocol::client::StreamError;

const MIB: usize = 1024 * 1024;
const DAY: u64 = 24 * 60 * 60;

/// The rendered fixture config's `payment.rate_per_mb`. The bait rate.
const BASE_RATE_PER_MB: u64 = 10;
/// The switched-to rate. Comfortably inside the deployed `[1, 1000]` on-chain
/// delivery band, so the daemon signs it unclamped.
const SWITCHED_RATE_PER_MB: u64 = 40;

/// Overall ceiling so an unbounded await fails fast with a clear message.
/// Cleanup (anvil kill, daemon kill) runs on drop even on timeout. The journey
/// runs a full deploy, two daemons, four commit-reveal challenges and several
/// time warps.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(1200);

#[tokio::test(flavor = "multi_thread")]
async fn real_node_responses_are_valid_on_chain_evidence() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-GOV-03 exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // Node A carries the phantom offense, node B the rate bait-and-switch. Two
    // operators rather than two offenses on one so each assertion reads against a
    // first-offense bond tier and an untouched `slashedAtEpoch`.
    let payload_a = vec![0xA1u8; 2 * MIB];
    let payload_b = vec![0xB2u8; 2 * MIB];
    let (node_a, hash_a) = NodeFixture::launch(&chain, "US", &payload_a).await?;
    let (node_b, hash_b) = NodeFixture::launch(&chain, "US", &payload_b).await?;
    let client = ClientFixture::new(&chain).await?;

    // ================================================================
    // Capture 1 — phantom announcement from node A.
    // ================================================================

    // A real paid delivery first. It proves A serves `H`, and — the part the
    // capture depends on — leaves behind a channel A's chain watcher has already
    // accepted, so the refusal below cannot be the pre-observation `UnknownChannel`
    // masquerading as evidence.
    let delivered = client.fetch(&chain, &node_a, hash_a).await?;
    anyhow::ensure!(
        delivered.bytes == payload_a,
        "node A must deliver H before we induce the phantom"
    );
    let channel_a = delivered.channel_id;

    // Anchor evidence timestamps to chain time. The judge's staleness bound (and
    // its future-skew guard) are what compare against `block.timestamp`; the 30s
    // window is computed purely between the two evidence timestamps and is
    // chain-time-independent. Anchoring both keeps the pair fresh as well as
    // in-window.
    let now_us = chain.head_timestamp().await? * 1_000_000;
    let probe_ts = now_us;
    let stream_ts = now_us + 1_000_000; // +1s, well inside the 30s window
    let stale_probe_ts = now_us - 40_000_000; // -40s, deliberately outside it

    // The out-of-window probe is captured FIRST, while A still holds `H` — it is
    // real daemon output (`has_blob: true`, signed), differing from the admissible
    // one only in the timestamp the requester chose.
    let stale_probe = client.probe_at(&node_a, hash_a, stale_probe_ts).await?;
    let probe_a = client.probe_at(&node_a, hash_a, probe_ts).await?;
    anyhow::ensure!(
        probe_a.body.has_blob && stale_probe.body.has_blob,
        "node A must announce H as held before eviction"
    );
    anyhow::ensure!(
        probe_a.body.rate_per_mb == BASE_RATE_PER_MB,
        "unexpected quoted rate {} from node A",
        probe_a.body.rate_per_mb
    );

    // The operator evicts what it just announced. Eviction is sticky and
    // authoritative in the client handler (#279), so the next stream refuses
    // rather than silently refilling from A's own filesystem origin.
    node_a
        .admin_client()?
        .evict(EvictRequest {
            hash: alloy::hex::encode(hash_a.as_bytes()),
            dry_run: false,
        })
        .await
        .context("admin evict H on node A")?;

    let stream_a = client
        .refused_stream(&chain, &node_a, channel_a, hash_a, stream_ts)
        .await?;
    anyhow::ensure!(
        !stream_a.body.ok,
        "the captured StreamResponse must be a refusal"
    );
    anyhow::ensure!(
        stream_a.body.hash == *hash_a.as_bytes(),
        "the refusal must answer for the probed hash"
    );
    // Pin the *cause*. Without this the journey would still pass if the eviction
    // silently no-op'd and the refusal were some other `ok: false` (an unknown
    // channel, say) — a slash landing for a reason the test never induced.
    anyhow::ensure!(
        stream_a.error == Some(StreamError::EvictedSinceProbe),
        "the refusal must be the eviction, got {:?}",
        stream_a.error
    );

    // ================================================================
    // Capture 2 — rate bait-and-switch from node B.
    // ================================================================

    let delivered_b = client.fetch(&chain, &node_b, hash_b).await?;
    anyhow::ensure!(
        delivered_b.bytes == payload_b,
        "node B must deliver its blob before we induce the switch"
    );
    let channel_b = delivered_b.channel_id;

    // A hash B has never held: its refusal is a clean cache miss, and no eviction
    // is needed to provoke it — so the *only* thing that changes between the probe
    // and the stream is the rate.
    let unheld = Hash::new(b"g-gov-03: a blob no node in this journey holds");
    let now_b_us = chain.head_timestamp().await? * 1_000_000;
    let probe_b = client.probe_at(&node_b, unheld, now_b_us).await?;
    anyhow::ensure!(
        probe_b.body.rate_per_mb == BASE_RATE_PER_MB,
        "node B must quote the configured rate before the switch, got {}",
        probe_b.body.rate_per_mb
    );

    // The switch: rewrite the operator's own config and hot-reload it. No
    // restart, no reconnect — the handlers read the rate through the atomic the
    // reload swaps.
    let reloaded = node_b.set_rate_per_mb(SWITCHED_RATE_PER_MB).await?;
    anyhow::ensure!(
        reloaded == SWITCHED_RATE_PER_MB,
        "daemon reported rate {reloaded} after reload, expected {SWITCHED_RATE_PER_MB}"
    );

    let stream_b = client
        .refused_stream(&chain, &node_b, channel_b, unheld, now_b_us + 1_000_000)
        .await?;
    anyhow::ensure!(
        stream_b.body.rate_per_mb > probe_b.body.rate_per_mb,
        "the daemon must have signed the raised rate: probe {} vs stream {}",
        probe_b.body.rate_per_mb,
        stream_b.body.rate_per_mb
    );
    // The refusal is the honest cache miss, not an eviction or a channel fault —
    // so the rate really is the only thing that moved between the two messages.
    anyhow::ensure!(
        stream_b.error == Some(StreamError::NotFound),
        "the refusal must be a plain miss, got {:?}",
        stream_b.error
    );

    // ================================================================
    // Negatives — the judge rejects real-but-inadmissible evidence.
    // ================================================================

    let node_a_id = B256::from_slice(node_a.node_id().as_bytes());
    let node_b_id = B256::from_slice(node_b.node_id().as_bytes());
    let bond = chain.challenge_bond().await?;

    // (1) Outside the 30s window. Both messages are genuine and correctly signed;
    // only the gap between them is inadmissible.
    let out_of_window = PrivateKeySigner::random();
    let err = chain
        .challenge_with_real_evidence(
            &out_of_window,
            node_a.operator_addr(),
            node_a_id,
            Offense::Phantom,
            EvidencePair {
                probe: &stale_probe,
                stream: &stream_a,
            },
            B256::repeat_byte(0x01),
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a 40s-apart probe/stream pair must not be slashable"))?;
    // Pinning the *typed* error is what makes a negative meaningful: an `is_err()`
    // alone would also be satisfied by a fixture bug (unfunded challenger, bad
    // nonce) that never reached the judge's checks at all.
    expect_revert_anyhow::<SlashJudgeRate::TimestampWindowViolated>(&err, "out-of-window pair")?;
    assert_challenge_cost_nothing(&chain, out_of_window.address(), bond).await?;

    // (2) Forged signature: the real probe body, re-signed by a key that is not
    // the operator's. `SignatureChecker` recovers a different address.
    let impostor = PrivateKeySigner::random();
    let forged = forge_probe_signature(&chain, &probe_a, &impostor)?;
    let forger = PrivateKeySigner::random();
    let err = chain
        .challenge_with_real_evidence(
            &forger,
            node_a.operator_addr(),
            node_a_id,
            Offense::Phantom,
            EvidencePair {
                probe: &forged,
                stream: &stream_a,
            },
            B256::repeat_byte(0x02),
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a probe signed by a non-operator must not be slashable"))?;
    expect_revert_anyhow::<SlashJudgeRate::InvalidProbeSignature>(&err, "forged probe signature")?;
    assert_challenge_cost_nothing(&chain, forger.address(), bond).await?;

    // (3) Neither failed challenge minted anything: A is still unslashed.
    anyhow::ensure!(
        chain.slashed_at_epoch(node_a.operator_addr()).await? == 0,
        "a rejected challenge must not stamp the vote-weight watermark"
    );

    // ================================================================
    // Positive — the real pairs are admissible.
    // ================================================================

    let slash_a = drive_slash(
        &chain,
        &node_a,
        node_a_id,
        Offense::Phantom,
        EvidencePair {
            probe: &probe_a,
            stream: &stream_a,
        },
        B256::repeat_byte(0x03),
    )
    .await
    .context("phantom challenge from real daemon evidence")?;

    let slash_b = drive_slash(
        &chain,
        &node_b,
        node_b_id,
        Offense::RateManipulation,
        EvidencePair {
            probe: &probe_b,
            stream: &stream_b,
        },
        B256::repeat_byte(0x04),
    )
    .await
    .context("rate challenge from real daemon evidence")?;

    // ================================================================
    // Finality — the 30-day filing window lapses with no appeal, and the escrow
    // splits 50% challenger / 50% burn.
    // ================================================================

    time::increase_time(chain.admin(), 31 * DAY).await?;
    assert_fifty_fifty_split(&chain, &slash_a).await?;
    assert_fifty_fifty_split(&chain, &slash_b).await?;

    Ok(())
}

/// A landed slash and the state captured around it.
struct LandedSlash {
    slash_id: U256,
    challenger: alloy::primitives::Address,
    amount: U256,
}

/// Submit `evidence` as `offense` against `node`, then assert the three
/// consequences the issue names for the positive path: the slash lands, the TOKEN
/// is *escrowed* rather than distributed, and the ADR-036 vote-weight watermark
/// is stamped.
async fn drive_slash(
    chain: &ChainFixture,
    node: &NodeFixture,
    node_id: B256,
    offense: Offense,
    evidence: EvidencePair<'_>,
    salt: B256,
) -> anyhow::Result<LandedSlash> {
    let operator = node.operator_addr();
    anyhow::ensure!(
        chain.slashed_at_epoch(operator).await? == 0,
        "operator must be unslashed before the challenge"
    );
    let bond_before = chain.active_bond(operator).await?;
    let escrow_before = chain.escrowed_total().await?;
    let challenger = PrivateKeySigner::random();
    let challenge_bond = chain.challenge_bond().await?;

    let slash_id = chain
        .challenge_with_real_evidence(&challenger, operator, node_id, offense, evidence, salt)
        .await?;

    // The slash landed and is attributed to this operator.
    let (rec_operator, _slashed_at, amount) = chain.slash_record(slash_id).await?;
    anyhow::ensure!(
        rec_operator == operator,
        "slash record names {rec_operator}, expected {operator}"
    );
    anyhow::ensure!(amount > U256::ZERO, "a slash must reduce a non-zero amount");
    // `slashAmount` is the *combined* reduction across active + unbonding bond,
    // and `_reduceBondAtTier` takes from active first — so this equality holds
    // because these fixture operators have no unbonding request in flight.
    anyhow::ensure!(
        chain.active_bond(operator).await? == bond_before - amount,
        "the operator's active bond must fall by exactly the slashed amount"
    );

    // Escrow-on-slash: the TOKEN is parked, not paid out, until finality.
    anyhow::ensure!(
        chain.escrowed_total().await? == escrow_before + amount,
        "the slashed TOKEN must be booked into escrow, not distributed"
    );
    anyhow::ensure!(
        chain.token_balance(challenger.address()).await? == challenge_bond,
        "a successful challenge returns the bond and pays nothing else before finality"
    );

    // ADR 036 § Slashing zero-out: the watermark is what zeroes the operator's
    // served-bytes vote weight.
    anyhow::ensure!(
        chain.slashed_at_epoch(operator).await? != 0,
        "a landed slash must stamp slashedAtEpoch (vote-weight zero-out)"
    );

    Ok(LandedSlash {
        slash_id,
        challenger: challenger.address(),
        amount,
    })
}

/// Finalize an unappealed slash and assert the ADR 026 distribution: 50% to the
/// recorded challenger, 50% burned. The burn leg is invisible as a transfer, so
/// it is read off TOKEN `totalSupply`.
async fn assert_fifty_fifty_split(chain: &ChainFixture, slash: &LandedSlash) -> anyhow::Result<()> {
    let challenger_before = chain.token_balance(slash.challenger).await?;
    let supply_before = chain.token_total_supply().await?;
    let escrow_before = chain.escrowed_total().await?;

    chain.finalize_unappealed_slash(slash.slash_id).await?;

    let challenger_share = slash.amount / U256::from(2u64);
    let burn_share = slash.amount - challenger_share;
    anyhow::ensure!(
        chain.token_balance(slash.challenger).await? == challenger_before + challenger_share,
        "the challenger must receive exactly 50% of the escrow"
    );
    anyhow::ensure!(
        chain.token_total_supply().await? == supply_before - burn_share,
        "the remaining 50% must be burned (supply reduction)"
    );
    anyhow::ensure!(
        chain.escrowed_total().await? == escrow_before - slash.amount,
        "the escrow must be released in full at finality"
    );
    Ok(())
}

/// Re-sign the *real* probe body with `impostor`'s key, leaving every other byte
/// untouched. The result is a structurally perfect `ProbeResponse` whose
/// signature recovers to the wrong address — the forgery a griefer would submit.
fn forge_probe_signature(
    chain: &ChainFixture,
    real: &ProbeResponse,
    impostor: &PrivateKeySigner,
) -> anyhow::Result<ProbeResponse> {
    let domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
    let data = ProbeSlashData {
        hash: B256::from(real.body.hash),
        has_blob: real.body.has_blob,
        rate_per_mb: real.body.rate_per_mb,
        timestamp_us: real.body.timestamp_us,
    };
    let sig = data.sign(impostor, &domain).context("forge probe sig")?;
    let mut forged = real.clone();
    forged.slash_sig = sig.as_bytes().to_vec();
    anyhow::ensure!(
        forged.slash_sig != real.slash_sig,
        "the forged signature must differ from the daemon's"
    );
    Ok(forged)
}

/// A challenge that fails on-chain verification must move no TOKEN: the bond is
/// pulled only inside `_resolve`, after every check has passed, so a reverting
/// challenge leaves the challenger holding exactly what it was armed with
/// (ADR 014 § Bond Handling — the challenger pays gas and nothing else).
async fn assert_challenge_cost_nothing(
    chain: &ChainFixture,
    challenger: alloy::primitives::Address,
    bond: U256,
) -> anyhow::Result<()> {
    let balance = chain.token_balance(challenger).await?;
    anyhow::ensure!(
        balance == bond,
        "a failed challenge must not transfer the bond: challenger holds {balance}, armed with {bond}"
    );
    Ok(())
}
