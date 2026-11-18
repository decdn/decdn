//! G-GOV-03 — real node responses as valid on-chain evidence (issue #1042).
//!
//! Every other "signed evidence" path in this harness cheats: it builds a
//! `StreamSlashData` in the test and signs it with `node.operator()`, because
//! the test holds the operator's key. That proves the *contract* accepts a
//! well-formed signature — which `SlashJudge.t.sol` already proves with
//! synthetic ones — and proves nothing about the daemon. This journey closes
//! that gap: every byte of the evidence came off the wire from a running
//! `decdn-node`, `slash_sig` included, and is forwarded verbatim.
//!
//! One node, one offense — **blacklist violation**. The node serves a held blob:
//! its signed `StreamResponse` commits to deliver it (`ok: true`). The blob's
//! hash is then on the GLOBAL blacklist with an entry that was enforceable
//! before that response, so the response is a served blacklisted blob — the
//! `submitBlacklistChallenge` evidence.
//!
//! The daemon signs only a request timestamp near its own wall clock, and the
//! judge slashes only a response stamped after the entry's `effectiveAt`
//! (`addedAt` plus the compliance window, ADR 011). The journey cannot wait out
//! the window against the wall clock, so it captures the response first and then
//! backdates the entry in chain time ([`ChainFixture::add_hash_global_at`]). On
//! chain, the node served a hash that had been blacklisted for a full compliance
//! window — exactly what a node that ignores the blacklist does.
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
//! - the real response with its signature swapped for a non-operator's is
//!   rejected (`InvalidResponseSignature`);
//! - a real, correctly-signed daemon *refusal* (`ok: false`) is rejected
//!   (`NotBlacklistViolation`). Its signature and hash pass, so this isolates the
//!   `ok` guard — the property that lets a node sign refusals freely;
//! - the real response replayed after a successful slash is rejected
//!   (`EvidenceAlreadyUsed`);
//! - a failed challenge moves no TOKEN. ADR 014 § Bond Handling: "a
//!   `submit*Challenge` that fails on-chain verification reverts and the
//!   challenger pays only gas; the bond is not transferred for failed
//!   verifications." There is no second-stage dispute that could claw it back
//!   after the fact. So the assertion is the behaviour of record: nothing is
//!   pulled, and no slash is minted.
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
    // `forged`/`forger`: the near-identical names pair a forged artifact with
    // the key that forged it, which reads clearer than a contrived rename.
    clippy::similar_names
)]

use std::time::Duration;

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::assert::expect_revert_anyhow;
use decdn_e2e::bindings::SlashJudgeBlacklist;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_e2e::time;
use decdn_incentive::slash_judge_domain;
use decdn_incentive::stream_sig::StreamSlashData;
use decdn_node::handlers::timestamp_skew::now_us;
use decdn_protocol::client::StreamResponse;

const MIB: usize = 1024 * 1024;
const DAY: u64 = 24 * 60 * 60;

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule). The
/// journey runs a full deploy, a daemon, and several commit-reveal challenges,
/// but each on-chain step is time-warped rather than waited out, so it stays
/// under the standard budget. Cleanup runs on drop even on timeout.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn real_node_responses_are_valid_on_chain_evidence() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-GOV-03 exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "sequential on-chain journey (capture → negatives → positive → finality) sharing one \
              anvil deployment: splitting it would either duplicate a ~60s deploy per stage or \
              thread the whole world through helpers without shortening the narrative"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    let payload = vec![0xA1u8; 2 * MIB];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;
    let client = ClientFixture::new(&chain).await?;
    let node_id = B256::from_slice(node.node_id().as_bytes());
    let hash_key = B256::from(*hash.as_bytes());

    // A real session: opens + funds a pool and warms `hash`, so the node has
    // observed the lane AND holds the blob — both preconditions for it to sign
    // a real `ok: true` delivery response below.
    let (session, _) = client.open_session(&chain, &node, hash).await?;

    // The real signed DELIVERY: the node commits to serve `hash`. Stamped from
    // the wall clock, which is the daemon's clock: the daemon signs a request
    // timestamp only within its clock-skew bound of its own clock.
    let stream_ts = now_us();
    let stream = client
        .capture_delivery_response(&session, hash, U256::ZERO, stream_ts)
        .await
        .context("capture the daemon's signed delivery")?;
    anyhow::ensure!(stream.body.ok, "the captured delivery must be ok:true");
    anyhow::ensure!(
        stream.body.hash == *hash.as_bytes(),
        "the delivery must answer for the requested hash"
    );

    // The real signed REFUSAL feeding the `ok`-guard negative: `ok: false` for
    // a hash the node does not hold.
    let absent = Hash::new(b"g-gov-03: never published, always refused");
    let refusal = client
        .refused_stream(&chain, &node, session.pool_id(), absent, now_us())
        .await
        .context("capture the daemon's signed refusal")?;
    anyhow::ensure!(!refusal.body.ok, "the captured refusal must be ok:false");

    // Blacklist `hash` with an entry enforceable a minute before the delivery:
    // `addedAt` sits a full compliance window plus that minute before it.
    let window = chain.compliance_window().await?;
    let added_at = stream_ts / 1_000_000 - window - 60;
    chain.add_hash_global_at(hash_key, added_at).await?;

    // ================================================================
    // Negatives — the judge rejects real-but-inadmissible evidence.
    // ================================================================

    let bond = chain.challenge_bond().await?;

    // (1) Forged signature: the real response body, but `slash_sig` re-signed by
    // a non-operator. The body (hence the evidenceHash) is unchanged, but the
    // signature check reverts before `_resolve` consults `usedEvidenceHash`, so
    // the real evidence stays spendable by the positive path below.
    let impostor = PrivateKeySigner::random();
    let forged = forge_stream_signature(&chain, &stream, &impostor)?;
    let forger = PrivateKeySigner::random();
    let err = chain
        .submit_blacklist_challenge(
            &forger,
            node.operator_addr(),
            node_id,
            &forged,
            B256::repeat_byte(0x02),
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a response signed by a non-operator must not slash"))?;
    expect_revert_anyhow::<SlashJudgeBlacklist::InvalidResponseSignature>(
        &err,
        "forged response signature",
    )?;
    assert_challenge_cost_nothing(&chain, forger.address(), bond).await?;

    // (2) A real, correctly-signed REFUSAL is inert. Its signature recovers to
    // the operator and its hash matches the challenged blob, so
    // `NotBlacklistViolation` here can only be the `ok` guard firing: a node
    // that declined to serve served nothing.
    let refusal_challenger = PrivateKeySigner::random();
    let err = chain
        .submit_blacklist_challenge(
            &refusal_challenger,
            node.operator_addr(),
            node_id,
            &refusal,
            B256::repeat_byte(0x08),
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a signed refusal must never be slashable"))?;
    expect_revert_anyhow::<SlashJudgeBlacklist::NotBlacklistViolation>(&err, "signed refusal")?;
    assert_challenge_cost_nothing(&chain, refusal_challenger.address(), bond).await?;

    // (3) Neither failed challenge minted anything: the operator is still unslashed.
    anyhow::ensure!(
        chain.slashed_at_epoch(node.operator_addr()).await? == 0,
        "a rejected challenge must not stamp the vote-weight watermark"
    );

    // ================================================================
    // Positive — the real response is admissible.
    // ================================================================

    let slash = drive_slash(&chain, &node, node_id, &stream, B256::repeat_byte(0x03))
        .await
        .context("blacklist challenge from real daemon evidence")?;

    // ================================================================
    // Replay — the same real response cannot slash twice.
    // ================================================================
    // Resubmitting the exact response under a *fresh* salt is rejected by
    // `EvidenceAlreadyUsed` — the dedup guard in `SlashJudge` itself (keyed on
    // the offense, operator and response), not in `CapacityBond`. The fresh
    // salt rules out the commit-reveal replay guard as the cause. It reverts at
    // `usedEvidenceHash` before the challenge bond is pulled, so it costs nothing.
    let replayer = PrivateKeySigner::random();
    let err = chain
        .submit_blacklist_challenge(
            &replayer,
            node.operator_addr(),
            node_id,
            &stream,
            B256::repeat_byte(0x07),
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a replayed response must not slash a second time"))?;
    expect_revert_anyhow::<SlashJudgeBlacklist::EvidenceAlreadyUsed>(&err, "replayed evidence")?;
    assert_challenge_cost_nothing(&chain, replayer.address(), bond).await?;

    // ================================================================
    // Finality — the 30-day filing window lapses with no appeal, and the escrow
    // splits 50% challenger / 50% burn.
    // ================================================================

    time::increase_time(chain.admin(), 31 * DAY).await?;
    assert_fifty_fifty_split(&chain, &slash).await?;

    Ok(())
}

/// A landed slash and the state captured around it.
struct LandedSlash {
    slash_id: U256,
    challenger: alloy::primitives::Address,
    amount: U256,
}

/// Submit `evidence` against `node`, then assert the three consequences the
/// issue names for the positive path: the slash lands, the TOKEN is *escrowed*
/// rather than distributed, and the ADR-036 vote-weight watermark is stamped.
async fn drive_slash(
    chain: &ChainFixture,
    node: &NodeFixture,
    node_id: B256,
    evidence: &StreamResponse,
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
        .submit_blacklist_challenge(&challenger, operator, node_id, evidence, salt)
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

    // Rounding matches the contract exactly, odd amounts included — and the
    // direction matters, so spell out why. `SlashEscrowLib._distribute` computes
    // `challengerShare = amount * CHALLENGER_BPS / BPS_DENOMINATOR` (= `amount *
    // 5000 / 10_000`, integer division, so the *challenger* leg floors) and gives
    // the remainder to the burn. Since 5000/10_000 is exactly 1/2, that is
    // `amount / 2` for every `amount`; an odd 7 splits 3 challenger / 4 burn under
    // both. Deliberately not read off the `SlashUpheld` event: asserting against
    // the contract's own emitted shares would only prove internal consistency,
    // where hard-coding 50/50 pins the ADR 026 policy itself.
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

/// Re-sign the *real* `StreamResponse` body with `impostor`'s key, leaving every
/// other byte untouched. The body (hence its EIP-712 struct hash and the
/// `evidenceHash`) is unchanged — only `slash_sig` recovers to the wrong address,
/// which is exactly what the judge's signature check must reject (#1378). The
/// forgery a griefer would submit.
fn forge_stream_signature(
    chain: &ChainFixture,
    real: &StreamResponse,
    impostor: &PrivateKeySigner,
) -> anyhow::Result<StreamResponse> {
    let domain = slash_judge_domain(chain.chain_id(), chain.addrs().slash_judge);
    let sig = StreamSlashData::from_response_body(&real.body)
        .sign(impostor, &domain)
        .context("forge stream sig")?;
    let mut forged = real.clone();
    forged.slash_sig = sig.as_bytes().to_vec();
    anyhow::ensure!(
        forged.slash_sig != real.slash_sig,
        "the forged stream signature must differ from the daemon's"
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
