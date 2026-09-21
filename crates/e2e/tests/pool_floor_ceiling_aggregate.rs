//! Live anvil-backed e2e for the per-pool floor-credit ceiling: the pool's
//! `remaining − M` bounds the AGGREGATE live, un-vouchered floor reservation
//! across DISTINCT signers, so the hard money envelope cannot be beaten by
//! spraying signer identities (ADR 003 § Pool solvency, stateful-B). THIS ceiling
//! is the on-chain-solvency bound that no fan-out of fresh signer keys can escape,
//! the complement to the per-signer live cap (which bounds one signer's concurrent
//! un-vouchered reservation).
//!
//! `crates/node/src/handlers/client/mod.rs` proves the invariant against fakes
//! (`pool_ceiling_still_bounds_the_aggregate_across_signers`: four signers each
//! take their own live window on a four-window pool, and the fifth is refused
//! `PoolExhausted`). This journey proves it end to end against a real anvil
//! chain, a real `decdn-node` daemon reading a real `getPool` `remaining`, and
//! the real paid `cdn/client/v1` wire.
//!
//! **The ceiling only binds under CONCURRENCY.** A sequential withhold-then-
//! disconnect releases its reservation on the disconnect, so it never grows the
//! pool's live total past one window — the reservation the pool ceiling bounds is
//! the LIVE, in-flight one. The journey therefore holds N streams OPEN at once,
//! each on its own distinct delegate signer, each parked mid-delivery awaiting a
//! voucher that never comes, so their reservations coexist on the pool's floor
//! accumulator. The pool is funded so `remaining − M` covers exactly
//! [`HELD_STREAMS`] windows, so:
//!
//! 1. **Admit up to the envelope.** Each of the [`HELD_STREAMS`] held streams, on
//!    a DISTINCT signer, is admitted and streamed its free floor. Every one sits
//!    at exactly its own one-window live reservation, so no per-signer live cap is
//!    exceeded — what admits them is pool headroom, and together they fill the
//!    envelope to the brim.
//!
//! 2. **Refuse the overflow across a fresh identity.** With all [`HELD_STREAMS`]
//!    reservations still live, one MORE stream on YET ANOTHER distinct signer is
//!    refused. Its own per-signer live cap is pristine (a fresh key, zero live), so
//!    the ONLY thing that can refuse it is the pool ceiling: the
//!    aggregate would exceed `remaining − M`. The refusal is `PoolExhausted`,
//!    which `FloorRefusal`→`ServeRejectReason` maps to `InsufficientDeposit` and
//!    `ServeRejectReason::wire_error` collapses onto the wire `NotFound` every
//!    reject reason shares.
//!
//! No admin surface exposes the pool's live floor accumulator (it is accounting,
//! not policy), so this journey asserts what is observable end to end: which
//! streams the node admits, which it refuses, and the per-reason reject counters
//! behind the collapsed `NotFound`. The blob every stream requests is a cache HIT,
//! so availability can never explain a refusal; a delta on
//! `decdn_serve_stream_rejected_insufficient_deposit_total` pins the refusal to
//! the pool ceiling, and a flat
//! `decdn_serve_stream_rejected_signer_floor_at_cap_total` proves it was NOT a
//! per-signer cap.
//!
//! Driven at the `PoolContext` / raw-wire layer rather than through
//! [`decdn_e2e::client::ClientFixture`]: the fixture's `open_pool_session` always
//! opens a FRESH pool self-owned by its one signer, so it cannot express "many
//! distinct signers spending against one shared pool" — the exact shape this
//! property needs. The lower-level pieces used here
//! (`decdn_client_pull::buyer_pool::open_pool`, `PoolContext`,
//! `decdn_incentive::Capability::sign`, and the raw `write_frame`/`read_frame`
//! wire helpers) are the same ones the fixture itself is built from.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` on
//! `PATH` and a built `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e pool_floor_ceiling_aggregate
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
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

/// The daemon's default `payment.rate_per_mb` (`crates/e2e/src/node.rs`'s
/// `render_config`). Left untouched — the floor cost the deposit is sized around
/// is derived from this exact value, so no `set_rate_per_mb` round trip is needed.
const RATE_PER_MB: u64 = 10;
/// A blob comfortably past one ramp-floor chunk
/// (`decdn_protocol::client::CHUNK_BYTES`): large enough that a held stream is
/// capped by the credit-window floor itself (it delivers exactly one window and
/// then parks awaiting a voucher), never by running out of content. Only the
/// lower bound is load-bearing; the margin above it costs nothing but transfer
/// time.
const BLOB_BYTES: usize = 4 * 1024 * 1024 + 65_536;
/// Owner-delegated finite spend cap on each delegate capability — far above one
/// floor's cost so it never itself binds; the pool ceiling (not this cap) is what
/// this journey exercises.
const DELEGATE_CAP_MICRO_USDC: u64 = 10_000_000;
const DELEGATE_EXPIRY_SECS: u64 = 3_600;
/// Fixed request timestamp; the node does not gate this path on freshness and
/// this journey never validates a signed response, so a constant suffices.
const TIMESTAMP_US: u64 = 0x00c0_ffe1;
/// Budget for the very first frame of a stream: generous enough to absorb the
/// pool-registration readiness settle on the FIRST held stream and a cache-hit
/// serve, not just network RTT.
const FIRST_FRAME_BUDGET: Duration = Duration::from_secs(30);
/// How long a held-open read waits for the NEXT frame of the credit window before
/// deciding the node has parked awaiting the voucher we never send. Kept below the
/// node's `VOUCHER_READ_TIMEOUT` (10s) so a park is observed as an idle gap, not a
/// connection close. In the happy path the loop breaks on delivered ≥ floor before
/// ever hitting this idle — the window's frames stream back-to-back — so it only
/// bounds the failure case where a stream parks SHORT of its floor.
const IDLE_BUDGET: Duration = Duration::from_secs(5);
/// How long the first request against the freshly-opened pool rides out the
/// node's pool-registration readiness window (its `getPool` view resolving the
/// pool) before treating a `NotFound` as a real refusal.
const READY_RETRY_BUDGET: Duration = Duration::from_secs(45);
/// How many streams to hold open at once. Sized to the pool ceiling: the deposit
/// below makes `remaining − M` cover exactly this many one-window reservations, so
/// this many DISTINCT-signer streams fit and the next is refused. Kept small so
/// every held stream is opened well inside the node's 10s `VOUCHER_READ_TIMEOUT`
/// (a parked stream's reservation is released once that elapses), leaving the
/// whole set live at the instant the overflow stream is refused.
const HELD_STREAMS: u64 = 2;
/// Per-signer LIVE concurrency cap, in windows, frozen far above anything this
/// journey draws. Each held stream is on its OWN signer and holds exactly one
/// window, so the per-signer cap is made deliberately non-binding: the ONLY gate
/// that can refuse the overflow stream is the pool ceiling, which is the whole
/// point.
const SIGNER_WINDOWS: u64 = 64;
/// The node-local reject counter behind the collapsed wire `NotFound` for a pool
/// ceiling refusal: `FloorRefusal::PoolExhausted` →
/// `ServeRejectReason::InsufficientDeposit`. A `0→1` delta across the overflow
/// request pins the refusal to the pool ceiling.
const INSUFFICIENT_DEPOSIT_METRIC: &str = "decdn_serve_stream_rejected_insufficient_deposit_total";
/// The per-signer live-cap reject counter. It must stay FLAT across the overflow
/// request: the refusal is the pool ceiling, not the per-signer cap, so spraying a
/// fresh identity cannot be what refused it.
const SIGNER_FLOOR_REJECT_METRIC: &str = "decdn_serve_stream_rejected_signer_floor_at_cap_total";
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn pool_ceiling_bounds_aggregate_live_reservation_across_distinct_signers()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("pool-ceiling aggregate e2e exceeded the overall timeout")??;
    Ok(())
}

/// The pool deposit: `M` plus exactly [`HELD_STREAMS`] one-window floors, so
/// `remaining − M` covers precisely the held set and the very next window
/// overflows it. `M` = [`DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC`] (1 USDC),
/// never overridden. One window at [`RATE_PER_MB`] is `floor_micro(RATE_PER_MB)`
/// (`credit_window(CHUNK_BYTES, 0) == CHUNK_BYTES` at ramp start, priced per MB),
/// which is exactly what a fresh held stream reserves — so this arithmetic is in
/// the same unit the node's floor accumulator counts in.
fn deposit_micro_usdc() -> U256 {
    let m = U256::from(DEFAULT_POOL_MIN_REMAINING_DEPOSIT_MICRO_USDC);
    let one_window = floor_micro(RATE_PER_MB);
    m + one_window * U256::from(HELD_STREAMS)
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: opening the pool, holding the \
              admit-to-the-brim set live, and refusing the overflow each depend on \
              the accumulator state the prior step left behind, so decomposing it \
              would thread that state through helpers without shortening the journey"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // The ramp floor a held stream delivers before parking, in the `usize` the
    // delivered-byte counter uses. The blob is deliberately much larger, so a
    // stream that parks short of this floor was capped by something other than the
    // credit window — a real bug, not the expected mid-delivery park.
    let floor_bytes = usize::try_from(CHUNK_BYTES).unwrap_or(usize::MAX);

    // One HIT blob (warmed into the node's cache at launch) that every stream in
    // this journey requests, so availability is never the reason for a refusal.
    let hit_blob = deterministic_blob(BLOB_BYTES, 0x5eed_0003);
    let (node, hit_hash) = NodeFixture::launch(&chain, "US", &hit_blob).await?;

    // Make the per-signer live cap deliberately non-binding: a large cap, and each
    // held stream on its own signer holding exactly one window. The ONLY gate left
    // able to refuse the overflow stream is the pool ceiling. Applied before any
    // pool activity; the restart reopens the same warm cache.
    node.set_pool_floor_signer(SIGNER_WINDOWS)
        .await
        .context("relax the per-signer live cap so only the pool ceiling binds")?;

    // The pool OWNER: funded via `ClientFixture` for its ETH/USDC/allowance
    // plumbing and its loopback iroh endpoint, reused directly (not through
    // `ClientFixture::fetch`, which always opens its own fresh, generously funded
    // pool) so this journey controls the exact deposit and shares one pool across
    // many distinct signers.
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

    // `HELD_STREAMS` distinct delegate signers to fill the envelope, plus ONE more
    // to overflow it — each a fresh, unfunded key holding its own owner-issued
    // capability naming it as `signer` on the SAME `pool_id`, so the node keys a
    // SEPARATE lane (and separate per-signer gates) for each. That the overflow is
    // a distinct identity is the invariant: fan-out cannot buy more envelope.
    let delegate_expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before epoch")?
        .as_secs()
        + DELEGATE_EXPIRY_SECS;
    let mut contexts = Vec::new();
    for _ in 0..=HELD_STREAMS {
        let (signer, cap) = delegate_lane(owner.signer(), pool_id, delegate_expiry, &voucher_dom)?;
        contexts.push(delegate_context(
            pool_id,
            owner.address(),
            chain.usdc(),
            deposit,
            node.operator_addr(),
            signer,
            cap,
            own_node_id,
            &bind_domain,
            &voucher_dom,
        )?);
    }

    let target = dial_target(&node).await?;

    // Fill the envelope to the brim: hold `HELD_STREAMS` streams OPEN at once, each
    // on its own distinct signer, each parked mid-delivery so its one-window
    // reservation stays LIVE on the pool's floor accumulator. The first ride the
    // pool-registration readiness window; the rest go straight through, the pool
    // now being known. Collected into `held` so the connections — and thus the
    // reservations — stay alive across the overflow assertion below.
    // Convert the window count to `usize` once, with context, so the indexing
    // below is infallible rather than papering over a bad conversion with a
    // `usize::MAX` index that would panic out of bounds.
    let held_count = usize::try_from(HELD_STREAMS).context("HELD_STREAMS does not fit in usize")?;
    let mut held: Vec<HeldStream> = Vec::new();
    for (round, ctx) in contexts.iter().enumerate().take(held_count) {
        let outcome = if round == 0 {
            hold_open_until_ready(owner.endpoint(), target.clone(), ctx, hit_hash, floor_bytes)
                .await?
        } else {
            open_and_hold(owner.endpoint(), target.clone(), ctx, hit_hash, floor_bytes).await?
        };
        match outcome {
            OpenOutcome::Held(stream) => {
                anyhow::ensure!(
                    stream.delivered >= floor_bytes,
                    "held stream {round} (of {HELD_STREAMS} that must fit the envelope) delivered \
                     only {} bytes before parking — short of the {floor_bytes}-byte ramp floor, so \
                     something other than the credit window capped it",
                    stream.delivered
                );
                held.push(stream);
            }
            OpenOutcome::Refused(reason) => anyhow::bail!(
                "held stream {round} (of {HELD_STREAMS} that must fit the envelope) was refused \
                 ({reason:?}); the pool ceiling should not bind until the aggregate reaches \
                 remaining − M"
            ),
        }
    }
    anyhow::ensure!(
        held.len() == held_count,
        "expected {HELD_STREAMS} live held streams, have {}",
        held.len()
    );

    // Overflow the envelope from a FRESH identity. Every one of the `HELD_STREAMS`
    // reservations above is still live (their connections are held in `held`), so
    // `remaining − M` is now fully committed. A stream on YET ANOTHER distinct
    // signer — pristine per-signer gates, zero live, empty bucket — can be refused
    // by NOTHING but the pool ceiling. It requests the SAME cached HIT, so
    // availability is not the reason either.
    let overflow_ctx = &contexts[held_count];
    let deposit_rejects_before = node.scrape_metric(INSUFFICIENT_DEPOSIT_METRIC).await?;
    let signer_rejects_before = node.scrape_metric(SIGNER_FLOOR_REJECT_METRIC).await?;
    let overflow = open_and_hold(
        owner.endpoint(),
        target,
        overflow_ctx,
        hit_hash,
        floor_bytes,
    )
    .await?;
    match overflow {
        OpenOutcome::Held(stream) => anyhow::bail!(
            "the overflow signer was served {} bytes while all {HELD_STREAMS} envelope \
             reservations were live; the pool ceiling should have refused it once the aggregate \
             reached remaining − M — spraying a fresh signer identity must not buy more envelope",
            stream.delivered
        ),
        OpenOutcome::Refused(StreamError::NotFound) => {}
        OpenOutcome::Refused(other) => anyhow::bail!(
            "the overflow signer was refused, but with {other:?} rather than the expected \
             `NotFound` (`ServeRejectReason::wire_error` collapses the pool-ceiling refusal onto \
             `NotFound`, same as every other reject reason)"
        ),
    }
    let deposit_rejects_after = node.scrape_metric(INSUFFICIENT_DEPOSIT_METRIC).await?;
    let signer_rejects_after = node.scrape_metric(SIGNER_FLOOR_REJECT_METRIC).await?;
    anyhow::ensure!(
        deposit_rejects_after == deposit_rejects_before + 1,
        "the overflow refusal must bump `{INSUFFICIENT_DEPOSIT_METRIC}` by exactly one \
         (before={deposit_rejects_before}, after={deposit_rejects_after}); that is the counter \
         behind a `PoolExhausted` → `InsufficientDeposit` refusal, and the blob is a cached HIT so \
         only the pool ceiling can refuse it"
    );
    anyhow::ensure!(
        signer_rejects_after == signer_rejects_before,
        "`{SIGNER_FLOOR_REJECT_METRIC}` must stay FLAT across the overflow refusal \
         (before={signer_rejects_before}, after={signer_rejects_after}); a bump would mean a \
         per-signer gate refused the fresh identity, but the invariant is that the POOL ceiling \
         bounds the aggregate regardless of which signer asks"
    );

    // Release the held reservations only now, after the ceiling has done its job.
    drop(held);
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
        spending_cap: DELEGATE_CAP_MICRO_USDC,
        pool_id,
        expiry,
    }
    .sign(owner, voucher_dom)
    .context("owner sign delegate capability")?;
    Ok((delegate, cap))
}

/// Build a delegate's [`PoolContext`]: pinned to `pool_id`/`provider`, an
/// untouched (zero) lane watermark, this signer's own client identity binding,
/// and the owner-issued capability naming it. Mirrors `ClientFixture`'s private
/// `open_pool_session`, generalized to an arbitrary (not necessarily owner)
/// signer.
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
    let state = BuyerPoolState::new(
        pool_id,
        Address::repeat_byte(0x9c),
        owner_addr,
        token,
        deposit,
    );
    Ok(
        PoolContext::for_pool(&state, Arc::new(delegate), voucher_dom.clone())
            .with_provider(provider, U256::ZERO, U256::ZERO)
            .with_client_binding(binding)
            .with_capability(capability),
    )
}

/// A live `cdn/client/v1` stream held open mid-delivery. The node signed
/// `ok: true` (which reserves one credit-window floor against the pool at the
/// admission gate), streamed the window, and is now parked awaiting a voucher that
/// never comes — so its reservation stays LIVE on the pool's floor accumulator for
/// as long as this value is kept alive (up to the node's `VOUCHER_READ_TIMEOUT`).
/// Dropping it closes the connection, which returns the node's serve task and
/// releases the reservation.
struct HeldStream {
    // The connection and its streams are held only to keep the node's live
    // reservation open; they are never read or written again. Dropping any of them
    // tears down the QUIC connection, so all three are retained.
    _conn: Connection,
    _send: SendStream,
    _recv: RecvStream,
    /// Bytes the node delivered before the read loop stopped — proof it served the
    /// real floor, not a degenerate zero-byte admit.
    delivered: usize,
}

/// The verdict of opening one `cdn/client/v1` stream: either the node admitted it
/// (`ok: true`, one floor now reserved) and it is held open, or it refused up
/// front.
enum OpenOutcome {
    Held(HeldStream),
    Refused(StreamError),
}

/// [`open_and_hold`], retried while the failure is `NotFound` and `deadline` has
/// not elapsed — riding out the node's pool-registration readiness window (its
/// `getPool` view resolving a freshly-opened pool) the same way
/// `ClientFixture::fetch`/`open_session` do for the production paths.
///
/// Only the FIRST request against a fresh pool needs this: while the pool view is
/// unresolved the node has no owner to verify the capability against, so it
/// registers no lane and refuses `NotFound` before any floor gate — no reservation
/// is taken by a readiness retry. Once a request succeeds the pool is known, and a
/// later `NotFound` is a real refusal, which is why the overflow assertion uses the
/// un-retried [`open_and_hold`] directly.
async fn hold_open_until_ready(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    hash: Hash,
    floor_bytes: usize,
) -> anyhow::Result<OpenOutcome> {
    let deadline = tokio::time::Instant::now() + READY_RETRY_BUDGET;
    loop {
        match open_and_hold(endpoint, target.clone(), ctx, hash, floor_bytes).await? {
            OpenOutcome::Refused(StreamError::NotFound)
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            outcome => return Ok(outcome),
        }
    }
}

/// Open one raw `cdn/client/v1` stream for `hash` under `ctx`, and — if the node
/// admits it — read the credit window it streams and then KEEP the connection
/// open, parked, without ever paying a voucher. The reservation the admission took
/// stays live for as long as the returned [`HeldStream`] is alive.
///
/// The read loop stops as soon as `floor_bytes` have arrived rather than waiting
/// for the node to fall idle: the window's frames stream back-to-back, so this
/// confirms the real floor was served AND returns fast, leaving the reservation
/// live well inside the node's 10s `VOUCHER_READ_TIMEOUT`. The per-frame
/// [`IDLE_BUDGET`] only bounds the failure case where a stream parks SHORT of its
/// floor. Mirrors `abandonment_bucket_throttle.rs`'s `open_and_withhold`, but
/// retains the connection instead of closing it.
async fn open_and_hold(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &PoolContext,
    hash: Hash,
    floor_bytes: usize,
) -> anyhow::Result<OpenOutcome> {
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
            spending_cap: signed.capability.spending_cap,
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
        return Ok(OpenOutcome::Refused(
            resp_ext.error.unwrap_or(StreamError::InternalError),
        ));
    }

    // Read the delivered window without ever paying. Break as soon as the floor is
    // in hand — the reservation is already live from admission, and the frames
    // stream back-to-back, so this does not wait out an idle gap on the happy path.
    // A park SHORT of the floor is the only case the `IDLE_BUDGET` gap catches.
    let mut delivered: usize = 0;
    while delivered < floor_bytes {
        match tokio::time::timeout(IDLE_BUDGET, decdn_protocol::read_frame(&mut recv)).await {
            Ok(Ok(frame)) => delivered += frame.len(),
            // Idle before the floor arrived, or a clean close: the stream parked
            // (or ended) short of its floor. Stop and let the caller's floor check
            // fail loudly.
            Err(_) => break,
            Ok(Err(decdn_protocol::FrameError::Io(e)))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Ok(Err(e)) => {
                conn.close(0u32.into(), b"held read failed");
                return Err(anyhow::anyhow!(
                    "held-stream read failed after {delivered} bytes: {e}"
                ));
            }
        }
    }

    // Keep the connection (and thus the node's live floor reservation) open.
    Ok(OpenOutcome::Held(HeldStream {
        _conn: conn,
        _send: send,
        _recv: recv,
        delivered,
    }))
}

/// The loopback dial target for `node`, at the identity it is serving under right
/// now (`admin_v1_health`, not the launch-frozen `NodeFixture::node_id()` —
/// mirrors `ClientFixture`'s private `target` helper).
async fn dial_target(node: &NodeFixture) -> anyhow::Result<EndpointAddr> {
    Ok(
        EndpointAddr::new(node.current_node_id().await?).with_ip_addr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, node.bind_port()),
        )),
    )
}

/// A deterministic pseudo-random blob of `len` bytes (xorshift32), so the blob is
/// large, non-trivially-compressible, and reproducible without depending on a
/// system RNG.
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
