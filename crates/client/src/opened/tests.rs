use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::Eip712Domain;
use decdn_protocol::client::StreamError;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode};

use super::fake_node::{Answer, FakeNode, Seen};
use super::{OpenedStream, UnverifiedAnswer, discovery_open};
use crate::{Connections, PoolContext, PullDeadlines, RateAboveCeiling, UpstreamRefused};

const OPEN: Duration = Duration::from_secs(5);

fn domain() -> Eip712Domain {
    decdn_incentive::slash_judge_domain(31_337, Address::repeat_byte(0x11))
}

fn blob() -> Vec<u8> {
    (0..70_000u32).map(|i| (i % 251) as u8).collect()
}

fn hash_of(blob: &[u8]) -> [u8; 32] {
    *blake3::hash(blob).as_bytes()
}

fn ctx(pool: u8) -> PoolContext {
    PoolContext::new(
        B256::repeat_byte(pool),
        U256::ZERO,
        Arc::new(PrivateKeySigner::random()),
        domain(),
    )
}

async fn client() -> anyhow::Result<(Endpoint, Connections)> {
    let ep = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await?;
    let conns = Connections::new(ep.clone());
    Ok((ep, conns))
}

async fn open_on(
    node: &FakeNode,
    conns: &Connections,
    ctx: &PoolContext,
    hash: [u8; 32],
) -> anyhow::Result<OpenedStream> {
    discovery_open(
        conns,
        &node.addr,
        node.operator_address(),
        ctx,
        &domain(),
        hash,
        decdn_protocol::client::NO_NAMESPACE,
        (0, 0),
        false,
        OPEN,
    )
    .await
}

/// Wait until `cond` holds, for at most two seconds.
async fn eventually(what: &str, cond: impl Fn() -> bool) -> anyhow::Result<()> {
    for _ in 0..200 {
        if cond() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("timed out waiting for {what}")
}

/// A discovery open of a held blob returns its verified answer: the quote,
/// the proven size, the coverage and the path RTT. The request names the
/// payer's pool and asks for no pull-through, and no voucher follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discovery_open_returns_the_verified_answer() -> anyhow::Result<()> {
    let blob = blob();
    let node = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 7,
        },
        domain(),
    )
    .await?;
    let (ep, conns) = client().await?;
    let ctx = ctx(1);
    let opened = open_on(&node, &conns, &ctx, hash_of(&blob)).await?;
    anyhow::ensure!(opened.served());
    anyhow::ensure!(opened.rate_per_mb() == 7);
    anyhow::ensure!(
        opened.proven_size().map(crate::ProvenSize::get) == Some(u64::try_from(blob.len())?)
    );
    anyhow::ensure!(!opened.coverage().is_empty());
    anyhow::ensure!(opened.rtt().is_some(), "a discovery open measures the RTT");
    anyhow::ensure!(opened.paid_by(&ctx));
    anyhow::ensure!(!opened.paid_by(&self::ctx(2)));
    let requests = node
        .seen
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let [(req, ext)] = requests.as_slice() else {
        anyhow::bail!("one request, got {}", requests.len());
    };
    anyhow::ensure!(req.pool_id == ctx.pool_id.0);
    anyhow::ensure!(!ext.pull_through, "a discovery open asks no pull-through");
    opened.cancel();
    ep.close().await;
    Ok(())
}

/// Cancelling a stream resets it at the node, and the shared connection stays
/// open: the next open on the node takes a new stream on it, with no dial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_resets_the_stream_and_keeps_the_connection() -> anyhow::Result<()> {
    let blob = blob();
    let node = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 7,
        },
        domain(),
    )
    .await?;
    let (ep, conns) = client().await?;
    let ctx = ctx(1);
    let first = open_on(&node, &conns, &ctx, hash_of(&blob)).await?;
    first.cancel();
    eventually("the node to see the reset", || {
        Seen::count(&node.seen.resets) == 1
    })
    .await?;
    anyhow::ensure!(node.connections_open(), "the cancel keeps the connection");
    let second = open_on(&node, &conns, &ctx, hash_of(&blob)).await?;
    anyhow::ensure!(second.served());
    anyhow::ensure!(conns.dials() == 1, "one dial for both opens");
    anyhow::ensure!(
        node.seen
            .conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == 1
    );
    second.cancel();
    ep.close().await;
    Ok(())
}

/// An answer whose coverage disagrees with its signed `ok` does not verify,
/// and a discovery open says so ([`UnverifiedAnswer`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_coverage_inconsistent_answer_is_unverified() -> anyhow::Result<()> {
    let blob = blob();
    let node = FakeNode::spawn(Answer::Inconsistent { blob: blob.clone() }, domain()).await?;
    let (ep, conns) = client().await?;
    let err = open_on(&node, &conns, &ctx(1), hash_of(&blob))
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("an inconsistent answer must not open"))?;
    anyhow::ensure!(
        err.downcast_ref::<UnverifiedAnswer>().is_some(),
        "tagged unverified: {err:#}"
    );
    anyhow::ensure!(format!("{err:#}").contains("coverage disagrees"), "{err:#}");
    ep.close().await;
    Ok(())
}

/// The adopting lane pays the stream: the pull's header carries the proven
/// size and quote, with no second open at the node. A context of another pool
/// cannot adopt it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn into_pull_adopts_the_stream_for_its_own_lane_only() -> anyhow::Result<()> {
    let blob = blob();
    let node = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 7,
        },
        domain(),
    )
    .await?;
    let (ep, conns) = client().await?;
    let ctx = ctx(1);
    let deadlines = PullDeadlines::new(OPEN, OPEN, 0)?;

    let other = open_on(&node, &conns, &ctx, hash_of(&blob)).await?;
    let wrong = self::ctx(2);
    let err = other
        .into_pull(&wrong, Arc::new(wrong.new_ledger()), 0, 0, deadlines)
        .err()
        .ok_or_else(|| anyhow::anyhow!("another pool's context must not adopt"))?;
    anyhow::ensure!(err.to_string().contains("not the lane's pool"), "{err:#}");

    let opened = open_on(&node, &conns, &ctx, hash_of(&blob)).await?;
    let (header, pull) = opened.into_pull(&ctx, Arc::new(ctx.new_ledger()), 0, 0, deadlines)?;
    anyhow::ensure!(header.total_bytes == u64::try_from(blob.len())?);
    anyhow::ensure!(header.rate_per_mb == 7);
    anyhow::ensure!(Seen::count(&node.seen.opens) == 2, "no second open");
    drop(pull);
    ep.close().await;
    Ok(())
}

/// The payer's checks run at adoption: a refusal is the typed refusal, and a
/// quote above the ceiling is refused before any voucher.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn into_pull_refuses_a_refusal_and_an_over_ceiling_quote() -> anyhow::Result<()> {
    let blob = blob();
    let deadlines = PullDeadlines::new(OPEN, OPEN, 0)?;
    let ctx = ctx(1);

    let refusing = FakeNode::spawn(
        Answer::Refuse {
            error: StreamError::NotFound,
            rate: 7,
        },
        domain(),
    )
    .await?;
    let (ep, conns) = client().await?;
    let refused = open_on(&refusing, &conns, &ctx, hash_of(&blob)).await?;
    anyhow::ensure!(!refused.served());
    let err = refused
        .into_pull(&ctx, Arc::new(ctx.new_ledger()), 0, 0, deadlines)
        .err()
        .ok_or_else(|| anyhow::anyhow!("a refusal is no pull"))?;
    anyhow::ensure!(
        err.downcast_ref::<UpstreamRefused>()
            .is_some_and(|r| matches!(r.error(), StreamError::NotFound)),
        "{err:#}"
    );

    let dear = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 50,
        },
        domain(),
    )
    .await?;
    let opened = open_on(&dear, &conns, &ctx, hash_of(&blob)).await?;
    let err = opened
        .into_pull(&ctx, Arc::new(ctx.new_ledger()), 0, 10, deadlines)
        .err()
        .ok_or_else(|| anyhow::anyhow!("an over-ceiling quote is no pull"))?;
    let over = err
        .downcast_ref::<RateAboveCeiling>()
        .ok_or_else(|| anyhow::anyhow!("typed: {err:#}"))?;
    anyhow::ensure!(over.quoted_rate_per_mb() == 50 && over.ceiling_rate_per_mb() == 10);
    anyhow::ensure!(Seen::count(&dear.seen.paid) == 0, "nothing paid");
    ep.close().await;
    Ok(())
}

/// A one-shot dial to a node beside the map's pooled connection to it opens
/// and answers: two connections from one endpoint to one node do not block
/// each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_one_shot_dial_beside_a_pooled_connection_answers() -> anyhow::Result<()> {
    let blob = blob();
    let node = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 7,
        },
        domain(),
    )
    .await?;
    let (ep, conns) = client().await?;
    let ctx = ctx(1);
    let pooled = open_on(&node, &conns, &ctx, hash_of(&blob)).await?;
    let one_shot = tokio::time::timeout(
        OPEN,
        crate::open_progressive_pull(
            &ep,
            node.addr.clone(),
            &ctx,
            Arc::new(ctx.new_ledger()),
            &domain(),
            node.operator_address(),
            hash_of(&blob),
            decdn_protocol::client::NO_NAMESPACE,
            0,
            crate::targets::micros_now(),
            0,
            0,
            PullDeadlines::new(OPEN, OPEN, 0)?,
            0,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the one-shot open hung beside the pooled connection"))??;
    anyhow::ensure!(one_shot.0.total_bytes == u64::try_from(blob.len())?);
    anyhow::ensure!(
        node.seen
            .conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == 2,
        "the one-shot open took a connection of its own"
    );
    drop(one_shot);
    pooled.cancel();
    ep.close().await;
    Ok(())
}
