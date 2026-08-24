//! Live anvil-backed e2e for the per-pool floor-credit bound: the seller keeps
//! serving a pool's free ramp-floor credit only while the pool's on-chain
//! `remaining` minus the configured refundable minimum `M`
//! (`pool_min_remaining_deposit_micro_usdc`) can still cover what is already
//! reserved or lost to it. `crates/node/tests/client_loopback.rs` proves this
//! bound against fakes (`FixedRemainingPoolView`, a stubbed `PoolView`); this
//! journey proves it end to end against a real anvil chain, a real
//! `decdn-node` daemon, and the real paid `cdn/client/v1` wire — including one
//! lane whose blob is not in the cache at open, so the fill path folds its
//! `dead_charge` into the same per-pool accumulator as the already-warm path.
//!
//! That lane reaches the client through the buffered `try_local_populate` route,
//! not `serve_leg`: `seed_origin_blob` writes `{H}` without a `{H}.obao4`, so the
//! serviceability probe finds no outboard and the range-pull leg is never chosen.
//! Nothing here exercises `serve_via_backend_origin` or the window pull-through,
//! and the burst measured below is a lower bound taken at `paid == 0`, where a
//! ramped credit window and one pinned at its floor are indistinguishable.
//!
//! Shape: one owner opens and funds a `PaymentPool` with a deposit sized to
//! fit exactly two ramp-floors of free credit above `M` (see the sizing
//! comment on [`deposit_micro_usdc`]). Two DISTINCT delegate signers — each
//! holding its own owner-issued [`decdn_incentive::Capability`] on the SAME
//! `pool_id` — each open one raw `cdn/client/v1` stream, let the node stream
//! them the free floor, and then disconnect WITHOUT ever paying a voucher
//! ("withhold"). The first lane pulls a blob the node already has cached (a
//! hit); the second pulls a blob seeded only into the node's opaque origin
//! backend (a genuine miss, forcing `serve_via_backend_origin`). Both fit the
//! sized budget and succeed. A third distinct lane then repeats the same
//! withhold against the same (already-committed) budget and is refused
//! `NotFound` — the wire code every `ServeRejectReason` collapses onto — which
//! is only possible if the two prior WITHHELD (never-redeemed) floors are
//! still bounding the pool, i.e. the accumulator is durable across
//! disconnects and shared across distinct signers and across the hit/miss
//! serve paths.
//!
//! No admin surface exposes the accumulator's internal `dead_charge` value
//! (it is accounting, not policy — see `crates/node/src/handlers/client/mod.rs`),
//! so this journey asserts what is observable end to end: which lanes the
//! node admits and which it refuses.
//!
//! Driven at the `PoolContext` / raw-wire layer rather than through
//! [`decdn_e2e::client::ClientFixture`]: the fixture's `open_pool_session`
//! always opens a FRESH pool self-owned by its one signer, so it cannot
//! express "three distinct signers spending against one shared pool" — the
//! exact shape a per-*pool* (not per-lane) bound needs to exercise. The
//! lower-level pieces used here (`decdn_client_pull::buyer_pool::open_pool`,
//! `PoolContext`, `decdn_incentive::Capability::sign`, and the raw
//! `write_frame`/`read_frame` wire helpers) are the same ones the fixture
//! itself is built from.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` on
//! `PATH` and a built `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e pool_floor_credit_bound
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client_pull::buyer_pool::open_pool;
use decdn_client_pull::{PoolContext, sign_client_binding};
use decdn_common::config::DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolState;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    Capability, SignedCapability, bind_node_id_domain, floor_micro, voucher_domain,
};
use decdn_protocol::client::{
    CHUNK_BYTES, ClientMessage, StreamError, StreamRequestExt, WireCapability,
};
use decdn_protocol::{ALPN_CLIENT, StreamRequest, encode_stream_request};
use iroh::{Endpoint, EndpointAddr};

/// The daemon's default `payment.rate_per_mb` (`crates/e2e/src/node.rs`'s
/// `render_config`). Left untouched — no `set_rate_per_mb` round trip is
/// needed since the budget below is derived from this exact value.
const RATE_PER_MB: u64 = 10;
/// A blob comfortably past one ramp-floor chunk
/// (`decdn_protocol::client::CHUNK_BYTES`): large enough that a withheld lane
/// is capped by the credit-window floor itself (delivered == reserved bytes, so
/// the accumulator folds exactly one floor's worth of `dead_charge` on
/// disconnect), never by running out of content early — see the module doc on
/// why an under-floor blob would under-count the fold. Only the lower bound is
/// load-bearing; the margin above it costs nothing but transfer time.
const BLOB_BYTES: usize = 4 * 1024 * 1024 + 65_536;
/// Owner-delegated finite spend cap on each delegate capability — far above
/// one floor's cost so it never itself binds; the pool deposit (not this cap)
/// is what this journey bounds.
const DELEGATE_CAP_MICRO_USDC: u64 = 10_000_000;
const DELEGATE_EXPIRY_SECS: u64 = 3_600;
/// Fixed request timestamp; the node does not gate this path on freshness and
/// this journey never validates a signed response, so a constant suffices.
const TIMESTAMP_US: u64 = 0x00c0_ffe1;
/// Budget for the very first frame of a stream: generous enough to absorb a
/// genuine cache-miss reactive backend fill (`try_local_populate`), not just
/// network RTT. Mirrors `ClientFixture::capture_delivery_wire`'s
/// `WIRE_TAP_FIRST_FRAME`.
const FIRST_FRAME_BUDGET: Duration = Duration::from_secs(30);
/// How long a withhold read waits between frames before deciding the node has
/// parked awaiting a voucher that will never come. Must stay below the node's
/// `VOUCHER_READ_TIMEOUT` (10s), matching `ClientFixture::capture_delivery_wire`'s
/// `WIRE_TAP_IDLE`.
const IDLE_BUDGET: Duration = Duration::from_secs(5);
/// How long the first two (expected-to-succeed) lanes ride out the node's
/// pool-registration readiness window (its `getPool` view resolving the
/// freshly-opened pool) before treating a `NotFound` as a real refusal.
const READY_RETRY_BUDGET: Duration = Duration::from_secs(45);
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn pool_floor_credit_bound_holds_across_distinct_lanes_and_a_real_miss() -> anyhow::Result<()>
{
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("pool floor-credit bound e2e exceeded the overall timeout")??;
    Ok(())
}

/// The pool deposit, sized to fit exactly two ramp-floors of free credit above
/// the node's default floor `M`.
///
/// `M` = [`DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC`] (1 USDC) — this
/// journey never overrides it, so the daemon serves under the exact same
/// value production ships. One ramp-floor at [`RATE_PER_MB`] is
/// `floor_micro(RATE_PER_MB)`: at cold start (`paid == 0`) the ramped credit
/// window always collapses to exactly one `CHUNK_BYTES` interval
/// regardless of `credit_max`/`credit_ramp_divisor` (ADR 003 §Credit window),
/// so every lane below reserves — and, once withheld, folds — exactly this
/// amount into the pool's `dead_charge` accumulator
/// (`crates/node/src/handlers/client/mod.rs::pool_budget_covers`). Two floors
/// plus a slack strictly smaller than a third floor means: the first two
/// distinct lanes both clear `remaining − M ≥ committed + floor`, and the
/// third cannot.
fn deposit_micro_usdc() -> U256 {
    let m = U256::from(DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC);
    let floor = floor_micro(RATE_PER_MB);
    // Slack under one floor, and derived from it rather than fixed: a withheld
    // lane folds slightly MORE than the floor, because the deliver phase checks
    // the window before each frame and so overshoots it by the one frame that
    // crosses. Half a floor absorbs that overshoot twice over while staying well
    // inside a third floor, so two lanes clear and the third cannot — at any
    // floor size.
    let slack = (floor / U256::from(2u64)).max(U256::from(1u64));
    m + floor * U256::from(2u64) + slack
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each lane depends on the pool/accumulator \
              state the prior lane left behind, so decomposing it would thread that state \
              through helpers without shortening the journey or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // The ramp floor a withheld lane is capped at, in the `usize` the
    // delivered-byte counters use. This — not a fraction of `BLOB_BYTES` — is
    // what a withheld lane's delivery is measured against: the blob is
    // deliberately much larger, so only the floor can explain where a lane parks.
    let floor_bytes = usize::try_from(CHUNK_BYTES).unwrap_or(usize::MAX);

    // A HIT blob (warmed into the node's cache at launch) and a MISS blob
    // (written only into the node's opaque origin backend, so it reaches a client
    // through a reactive origin fill — the buffered `try_local_populate` route,
    // since `seed_origin_blob` writes no outboard for the range-pull leg to use).
    // Both exceed one ramp-floor interval so a withheld lane is capped by the
    // credit window itself, not by running out of content (see
    // `deposit_micro_usdc`'s doc comment).
    let hit_blob = deterministic_blob(BLOB_BYTES, 0x5eed_0001);
    let (node, hit_hash) = NodeFixture::launch(&chain, "US", &hit_blob).await?;
    let miss_blob = deterministic_blob(BLOB_BYTES, 0x5eed_0002);
    let miss_hash = node
        .seed_origin_blob(&miss_blob)
        .context("seed origin-only miss blob")?;
    anyhow::ensure!(
        miss_hash != hit_hash,
        "hit and miss blobs must hash differently, or the miss lane would silently hit the cache"
    );

    // The pool OWNER: funded via `ClientFixture` for its ETH/USDC/allowance
    // plumbing and its loopback iroh endpoint, reused directly (not through
    // `ClientFixture::fetch`, which always opens its own fresh, generously
    // funded pool) so this journey controls the exact deposit.
    let owner = ClientFixture::new(&chain).await?;
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
    let bind_domain = bind_node_id_domain(chain.chain_id(), chain.addrs().capacity_bond);
    let own_node_id = alloy::primitives::B256::from(*owner.endpoint().id().as_bytes());

    let contract = PaymentPool::new(
        chain.addrs().payment_pool,
        chain.provider_for(owner.signer()),
    );
    let deposit = deposit_micro_usdc();
    let opened = open_pool(
        &contract,
        Arc::clone(owner.signer()),
        &voucher_dom,
        chain.usdc(),
        owner.address(),
        deposit,
    )
    .await
    .context("owner open pool")?;
    let pool_id = opened.state.pool_id;

    // Lane 1 — the OWNER's own self-issued capability (from `open_pool`), on
    // the HIT blob.
    let owner_ctx = opened
        .ctx
        .with_provider(node.operator_addr(), U256::ZERO, U256::ZERO)
        .with_client_binding(sign_client_binding(
            owner.signer(),
            own_node_id,
            &bind_domain,
        )?)
        .with_capability(opened.capability);

    // Lanes 2 and 3 — two DISTINCT delegate signers, neither funded with any
    // ETH or USDC (mirrors `cli_fetch_delegated.rs`: a delegate signs vouchers
    // and its client binding off-chain and issues no on-chain transaction of
    // its own). Each holds its own owner-issued, finitely-capped
    // `Capability` naming it as `signer` on the SAME `pool_id` — the
    // per-*pool*, not per-lane, budget this journey bounds.
    let delegate_expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before epoch")?
        .as_secs()
        + DELEGATE_EXPIRY_SECS;
    let (delegate_a, cap_a) =
        delegate_lane(owner.signer(), pool_id, delegate_expiry, &voucher_dom)?;
    let delegate_a_ctx = delegate_context(
        pool_id,
        owner.address(),
        chain.usdc(),
        deposit,
        node.operator_addr(),
        delegate_a,
        cap_a,
        own_node_id,
        &bind_domain,
        &voucher_dom,
    )?;
    let (delegate_b, cap_b) =
        delegate_lane(owner.signer(), pool_id, delegate_expiry, &voucher_dom)?;
    let delegate_b_ctx = delegate_context(
        pool_id,
        owner.address(),
        chain.usdc(),
        deposit,
        node.operator_addr(),
        delegate_b,
        cap_b,
        own_node_id,
        &bind_domain,
        &voucher_dom,
    )?;

    let target = dial_target(&node).await?;

    // Lane 1 (owner, HIT): first request against this pool, so ride out the
    // node's pool-registration readiness window before deciding a `NotFound`
    // means anything more than "not caught up yet".
    let lane1 =
        withhold_until_ready(owner.endpoint(), target.clone(), &owner_ctx, hit_hash).await?;
    let lane1_bytes = match lane1 {
        WithholdOutcome::Delivered { bytes } => bytes,
        WithholdOutcome::Refused(reason) => anyhow::bail!(
            "lane 1 (owner, HIT, first two of two budgeted floors) was refused ({reason:?}); \
             the sizing in `deposit_micro_usdc` assumes this always clears the budget"
        ),
    };
    anyhow::ensure!(
        lane1_bytes >= floor_bytes,
        "lane 1 delivered only {lane1_bytes} bytes before parking — short of the \
         {floor_bytes}-byte ramp floor, so something other than the credit window \
         capped it"
    );

    // Lane 2 (distinct delegate signer, MISS): the pool is already known to
    // the node (lane 1 succeeded), so no readiness retry is needed here — any
    // `NotFound` would be the real floor refusal, which the budget does not
    // yet permit. The generous `FIRST_FRAME_BUDGET` absorbs the genuine
    // reactive origin fill this lane forces.
    let lane2 =
        open_and_withhold(owner.endpoint(), target.clone(), &delegate_a_ctx, miss_hash).await?;
    let lane2_bytes = match lane2 {
        WithholdOutcome::Delivered { bytes } => bytes,
        WithholdOutcome::Refused(reason) => anyhow::bail!(
            "lane 2 (distinct delegate, MISS, second of two budgeted floors) was refused \
             ({reason:?}); the shared `serve_leg` miss path must reserve and fold its floor \
             exactly like the hit path does"
        ),
    };
    anyhow::ensure!(
        lane2_bytes >= floor_bytes,
        "lane 2 (the real cache-miss lane) delivered only {lane2_bytes} bytes before parking — \
         short of the {floor_bytes}-byte ramp floor, so something other than the credit \
         window capped it (e.g. a degenerate near-instant refusal)"
    );

    // Lane 3 (a THIRD distinct delegate signer, HIT — same blob as lane 1, so
    // availability can never be the reason for a refusal here): both budgeted
    // floors are now committed as `dead_charge` from lanes 1 and 2's
    // withholds, so this lane must be refused. This is the assertion the
    // whole journey exists for: the bound is durable across disconnects
    // (lanes 1 and 2 already closed their connections) and shared across
    // distinct signers and across the hit/miss serve paths — not merely a
    // per-lane or per-signer cap.
    let lane3 = open_and_withhold(owner.endpoint(), target, &delegate_b_ctx, hit_hash).await?;
    match lane3 {
        WithholdOutcome::Delivered { bytes } => anyhow::bail!(
            "lane 3 (a THIRD distinct delegate) was served {bytes} bytes; the per-pool \
             floor-credit accumulator should have refused it — the two prior withheld floors \
             from lanes 1 and 2 must still be bounding the pool"
        ),
        WithholdOutcome::Refused(StreamError::NotFound) => {}
        WithholdOutcome::Refused(other) => anyhow::bail!(
            "lane 3 was refused, but with {other:?} rather than the expected `NotFound` \
             (`ServeRejectReason::wire_error` collapses the floor-exhaustion refusal onto \
             `NotFound`, same as every other reject reason)"
        ),
    }

    drop(node);
    Ok(())
}

/// Mint a distinct delegate signer plus its owner-issued, finitely-capped
/// [`Capability`] on `pool_id`. Deliberately unfunded (no ETH, no USDC): the
/// delegate never sends an on-chain transaction of its own.
fn delegate_lane(
    owner: &PrivateKeySigner,
    pool_id: alloy::primitives::B256,
    expiry: u64,
    voucher_dom: &alloy::dyn_abi::Eip712Domain,
) -> anyhow::Result<(PrivateKeySigner, SignedCapability)> {
    let delegate = PrivateKeySigner::random();
    let cap = Capability {
        signer: delegate.address(),
        spending_cap: U256::from(DELEGATE_CAP_MICRO_USDC),
        pool_id,
        expiry,
    }
    .sign(owner, voucher_dom)
    .context("owner sign delegate capability")?;
    Ok((delegate, cap))
}

/// Build a delegate's [`PoolContext`]: pinned to `pool_id`/`provider`, an
/// untouched (zero) lane watermark, this signer's own client identity
/// binding, and the owner-issued capability naming it. Mirrors
/// `ClientFixture`'s private `open_pool_session`, generalized to an arbitrary
/// (not necessarily owner) signer.
#[allow(clippy::too_many_arguments)]
fn delegate_context(
    pool_id: alloy::primitives::B256,
    owner_addr: Address,
    token: Address,
    deposit: U256,
    provider: Address,
    delegate: PrivateKeySigner,
    capability: SignedCapability,
    own_node_id: alloy::primitives::B256,
    bind_domain: &alloy::dyn_abi::Eip712Domain,
    voucher_dom: &alloy::dyn_abi::Eip712Domain,
) -> anyhow::Result<PoolContext> {
    let binding = sign_client_binding(&delegate, own_node_id, bind_domain)?;
    let state = BuyerPoolState::new(pool_id, owner_addr, token, deposit);
    Ok(
        PoolContext::for_pool(&state, Arc::new(delegate), voucher_dom.clone())
            .with_provider(provider, U256::ZERO, U256::ZERO)
            .with_client_binding(binding)
            .with_capability(capability),
    )
}

/// Outcome of a single withheld `cdn/client/v1` stream: either the node
/// answered `ok: true` and streamed some bytes before parking awaiting a
/// voucher that never comes, or it refused up front.
#[derive(Debug)]
enum WithholdOutcome {
    Delivered { bytes: usize },
    Refused(StreamError),
}

/// [`open_and_withhold`], retried while the failure is `NotFound` and
/// `deadline` has not elapsed — riding out the node's pool-registration
/// readiness window (its `getPool` view resolving a freshly-opened pool) the
/// same way `ClientFixture::fetch`/`open_session` do for the production
/// paths.
async fn withhold_until_ready(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    hash: Hash,
) -> anyhow::Result<WithholdOutcome> {
    let deadline = tokio::time::Instant::now() + READY_RETRY_BUDGET;
    loop {
        match open_and_withhold(endpoint, target.clone(), ctx, hash).await? {
            WithholdOutcome::Refused(StreamError::NotFound)
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            outcome => return Ok(outcome),
        }
    }
}

/// Open one raw `cdn/client/v1` stream for `hash` under `ctx`, read whatever
/// the node sends, and NEVER pay a voucher — either the node refuses up front
/// (`StreamResponse { ok: false, .. }`), or it streams bytes up to the
/// credit-window floor and then parks; either way this closes the connection
/// once the node has said everything it is going to say, without ever
/// advancing the lane's voucher watermark. Mirrors
/// `ClientFixture::capture_delivery_wire`, generalized to report the open
/// verdict rather than the raw frames.
async fn open_and_withhold(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    hash: Hash,
) -> anyhow::Result<WithholdOutcome> {
    let conn = endpoint
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi: {e}"))?;

    let req = StreamRequest {
        hash: *hash.as_bytes(),
        namespace_id: [0u8; 32],
        pool_id: ctx.pool_id.into(),
        byte_offset: 0,
        byte_len: 0,
        timestamp_us: TIMESTAMP_US,
    };
    let ext = StreamRequestExt {
        binding: ctx.client_binding.clone(),
        capability: ctx.capability.as_ref().map(|signed| WireCapability {
            spending_cap: signed.capability.spending_cap.to_be_bytes(),
            expiry: signed.capability.expiry,
            owner_signature: signed.signature.as_bytes().to_vec(),
        }),
    };
    let payload = encode_stream_request(&req, Some(&ext)).context("encode StreamRequest")?;
    decdn_protocol::write_frame(&mut send, &payload)
        .await
        .context("write StreamRequest")?;

    let first = tokio::time::timeout(FIRST_FRAME_BUDGET, decdn_protocol::read_frame(&mut recv))
        .await
        .context("node sent no open frame within budget")?
        .context("read open frame")?;
    let (msg, tail) =
        decdn_protocol::decode_message::<ClientMessage>(&first).context("decode open frame")?;
    let ClientMessage::StreamResponse(resp) = msg else {
        anyhow::bail!("expected a StreamResponse open frame, got a different message");
    };
    // The refusal code rides in the trailing extension (ADR 013 §Tier 1), so the
    // open frame is read two-phase.
    let resp_ext =
        decdn_protocol::parse_stream_response_ext(tail).context("decode open frame extension")?;
    if !resp.body.ok {
        conn.close(0u32.into(), b"refused");
        return Ok(WithholdOutcome::Refused(
            resp_ext.error.unwrap_or(StreamError::InternalError),
        ));
    }

    // Drain further frames (ChunkData) without ever paying, until the node
    // falls idle awaiting the voucher we never send, or closes on its own.
    let mut delivered: usize = 0;
    loop {
        match tokio::time::timeout(IDLE_BUDGET, decdn_protocol::read_frame(&mut recv)).await {
            Ok(Ok(frame)) => delivered += frame.len(),
            // Idle: the node parked awaiting a voucher — the withhold point.
            Err(_) => break,
            Ok(Err(decdn_protocol::FrameError::Io(e)))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Ok(Err(e)) => {
                conn.close(0u32.into(), b"withhold read failed");
                return Err(anyhow::anyhow!(
                    "withhold read failed after {delivered} bytes: {e}"
                ));
            }
        }
    }
    conn.close(0u32.into(), b"withhold complete");
    Ok(WithholdOutcome::Delivered { bytes: delivered })
}

/// The loopback dial target for `node`, at the identity it is serving under
/// right now (`admin_v1_health`, not the launch-frozen `NodeFixture::node_id()`
/// — mirrors `ClientFixture`'s private `target` helper).
async fn dial_target(node: &NodeFixture) -> anyhow::Result<EndpointAddr> {
    Ok(
        EndpointAddr::new(node.current_node_id().await?).with_ip_addr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
        )),
    )
}

/// A deterministic pseudo-random blob of `len` bytes (xorshift32), so the
/// two blobs in this journey are large, non-trivially-compressible, and
/// reproducible without depending on a system RNG.
fn deterministic_blob(len: usize, seed: u32) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x = seed;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}
