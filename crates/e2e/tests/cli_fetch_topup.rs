//! Live anvil-backed e2e for the CLI `decdn fetch` buyer auto-`topUp` path
//! (issue #1103). The mirror of `crates/node/tests/anvil_settlement_e2e.rs:937`
//! (which asserts the *node* buyer's `top_up` raises the on-chain deposit and
//! persists it) but for the CLI fetch buyer's stack: the shared
//! [`decdn_client_pull::buyer_pool::top_up`] kernel driving the same
//! persistent [`RedbBuyerPoolStore`] the CLI fetch path uses.
//!
//! Shape: deploy the protocol, have a buyer open a pool and record it in a redb
//! store, advance the persisted lane watermark so the pool's remaining deposit
//! runs low (the sustained-fetch scenario that strands a small pool today), then
//! `top_up` on-chain, credit the returned amount into the local record, and
//! assert BOTH the on-chain `getPool().deposit` and the persisted record's
//! deposit rose by the added amount.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_topup
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

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_bao_range::align_range;
use decdn_cache::Hash;
use decdn_client_pull::buyer_pool::{ensure_allowance, open_pool, top_up};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{LaneKey, voucher_domain};
// The denominator of `next_voucher`'s `ceil(bytes * rate / MB)` pricing, taken
// from the protocol rather than re-spelled locally: a hand-copied 1024 * 1024
// would keep passing if the protocol constant ever moved.
use decdn_protocol::MB_BYTES;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_auto_topup_raises_and_persists_deposit() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch top-up e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's pool/\
              deposit state, so decomposing it would thread state through helpers without reducing \
              the journey's length or making it easier to follow"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // The provider node the lane pays. A pool names no provider at open, so the
    // provider address only labels the `(signer, provider)` voucher lane here;
    // no onboarding is needed for the buyer to open and top up its own pool.
    let provider_signer =
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).context("build provider signer")?;
    let provider_addr = provider_signer.address();

    // Buyer/client account: gas + enough USDC for the deposit and a full refill.
    let buyer_signer =
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x22)).context("build buyer signer")?;
    let buyer_addr = buyer_signer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await?;

    let buyer_provider = chain.provider_for(&buyer_signer);
    let pool = PaymentPool::new(chain.addrs().payment_pool, buyer_provider.clone());
    // Unlimited standing allowance: covers both the initial `openPool` deposit
    // and the later refill `topUp` without re-approving (a `--max-approve`
    // buyer). Passing `None` selects the max-approval path.
    ensure_allowance(
        &buyer_provider,
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_pool,
        None,
    )
    .await
    .context("approve PaymentPool")?;

    let deposit = U256::from(DEPOSIT_MICRO_USDC);
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);

    let opened = open_pool(
        &pool,
        Arc::new(buyer_signer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        deposit,
    )
    .await
    .context("open buyer pool")?;
    let pool_id = opened.state.pool_id;
    // The buyer self-signs, so the lane's signer is its own address; the provider
    // is the node the lane pays.
    let lane = LaneKey {
        pool_id,
        signer: buyer_addr,
        provider: provider_addr,
    };

    // The persistent store the CLI fetch path uses. The store enforces a
    // `0o700` data dir; `tempdir()` defaults to `0o755`, so tighten it first.
    // Unix-only (the `0o700` mode and the store's enforcement are POSIX); the
    // rest of the journey is platform-independent.
    let dir = tempfile::tempdir().context("tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod data dir 0o700")?;
    let store = RedbBuyerPoolStore::open(dir.path()).context("open redb buyer store")?;
    store.record(&opened.state).context("record pool")?;

    // Simulate a sustained series of fetches that spends the pool down to a
    // remaining deposit of 1 µUSDC — well below the CLI's low-water mark — by
    // advancing the persisted lane watermark.
    let prior_amount = deposit - U256::from(1u64);
    let outcome = store
        .advance_progress(buyer_addr, pool_id, lane, U256::from(1u64), prior_amount)
        .context("advance watermark")?;
    anyhow::ensure!(
        matches!(
            outcome,
            decdn_incentive::buyer_pool::AdvanceOutcome::Advanced
        ),
        "watermark advance failed: {outcome:?}"
    );

    // The CLI's refill policy tops up by the shortfall that restores the
    // remaining deposit to the configured target (here: `deposit - remaining`,
    // remaining == 1). Drive the shared kernel with that amount, then credit the
    // measured on-chain amount into the local record via `add_deposit`.
    let remaining = deposit - prior_amount; // == 1
    let additional = deposit - remaining; // restore to a full `deposit`
    let credited = top_up(&pool, pool_id, additional).await.context("top_up")?;
    let deposit_outcome = store
        .add_deposit(buyer_addr, pool_id, credited)
        .context("credit top-up into the local record")?;
    anyhow::ensure!(
        matches!(
            deposit_outcome,
            decdn_incentive::buyer_pool::DepositOutcome::Added(_)
        ),
        "add_deposit did not credit the pool: {deposit_outcome:?}"
    );

    let expected = deposit + credited;
    let onchain = pool
        .getPool(pool_id)
        .call()
        .await
        .context("getPool")?
        .deposit;
    anyhow::ensure!(
        U256::from(onchain) == expected,
        "topUp must raise the on-chain deposit to {expected}, got {onchain}"
    );
    let persisted = store
        .get_by_pool_id(pool_id)
        .context("re-read persisted pool")?
        .ok_or_else(|| anyhow::anyhow!("buyer pool vanished after top_up"))?
        .deposit;
    anyhow::ensure!(
        persisted == expected,
        "top_up must persist the raised deposit ({expected} µUSDC), got {persisted}"
    );

    // The lane watermark must be untouched by the top-up (a reused pool resumes
    // from the same cumulative bytes/amount, only with more headroom).
    let after = store
        .get_by_pool_id(pool_id)
        .context("re-read watermark")?
        .ok_or_else(|| anyhow::anyhow!("buyer pool vanished"))?;
    let progress = after
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("lane watermark vanished"))?;
    anyhow::ensure!(
        progress.last_amount == prior_amount && progress.last_bytes == U256::from(1u64),
        "top_up must not disturb the lane's cumulative voucher watermark"
    );

    Ok(())
}

const TOPUP_KEYSTORE_PASSWORD: &str = "topup-e2e-password";

/// Cumulative bytes billed to `provider` on the persisted pool's lane, or `0`
/// before any pool has been recorded. Mirrors `cli_fetch_resume.rs`'s helper of
/// the same shape: one buyer signs one pool, so the highest lane watermark naming
/// `provider` is the cumulative bytes billed to it.
fn billed_bytes(data_dir: &std::path::Path, provider: Address) -> anyhow::Result<u64> {
    let Ok(store) = RedbBuyerPoolStore::open(data_dir) else {
        return Ok(0);
    };
    let mut billed = U256::ZERO;
    for pool in store.load_all().context("load buyer pools")?.pools {
        for (lane, progress) in pool.lanes() {
            if lane.provider == provider {
                billed = billed.max(progress.last_bytes);
            }
        }
    }
    Ok(u64::try_from(billed).unwrap_or(u64::MAX))
}

/// The `decdn fetch` argv (after the `fetch` subcommand) for a reactive top-up
/// journey, with the pool's single `--working-deposit-micro-usdc` as a parameter.
/// The pool opens at this deposit and every reactive top-up restores it back
/// toward it, so a blob whose cost exceeds it exhausts the deposit mid-stream and
/// the reactive leg tops up. `--capacity-bond-address` is required: it is the
/// EIP-712 `verifyingContract` the buyer signs its ADR 005 client identity
/// binding against, and the node refuses to serve a paid request that carries no
/// verified binding — even for a blob it already holds.
fn topup_fetch_argv_with_deposits(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
    working_deposit_micro_usdc: u64,
) -> Vec<String> {
    vec![
        "--hash".into(),
        hash.to_hex(),
        "-o".into(),
        out.display().to_string(),
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--working-deposit-micro-usdc".into(),
        working_deposit_micro_usdc.to_string(),
    ]
}

// ---- Multi-interval reactive top-up: no double-pay, no under-pay ----
//
// The test above bakes the pool's very FIRST voucher into `InsufficientDeposit`
// on a fresh pool — no prior accepted voucher exists, so this is the shallowest
// possible exercise of the reactive branch. This test drives the deeper case:
// several whole chunks get delivered AND ACCEPTED first, and only the
// NEXT one exhausts the deposit.
//
// Reaching the top-up path here depends on `genuine_exhaustion` (client-pull's
// advancement-based bundle check): once any voucher has been accepted, the node
// attaches a `WatermarkBundle` to every subsequent watermark-gated rejection it
// can (`watermark_bundle_for_reject`), including a perfectly ordinary exhaustion.
// `genuine_exhaustion` distinguishes "bundle reports something AHEAD of what we
// already hold" (desync — reseed) from "bundle just echoes our own
// already-committed watermark" (not a desync — the exhaustion is real), by
// comparing the bundle's cumulative amount against `ledger.committed()`. That
// keeps real exhaustion on the top-up path instead of the resync path, which
// cannot fix a genuinely short deposit and would fail the fetch after burning
// `MAX_RESUME_ATTEMPTS`.
//
// This also exercises the resume offset: the resume lands at the CONTENT paid
// frontier — `content_paid_frontier(fetch_start_offset, total_bytes,
// committed.bytes_now - committed.bytes_at_start)` — rather than the raw on-disk
// length OR the naive `fetch_start + wire_delta`. Vouchers pay for WIRE bytes
// (bao content plus interleaved proof, ADR 038), so `committed.bytes` is a WIRE
// watermark; mapping it back through the bao tree lands the resume on the largest
// content chunk-group boundary provably inside the paid wire. The resume must
// re-fetch and pay for exactly the delivered-but-unpaid tail — no more
// (double-pay), no less (under-pay). Treating the wire watermark as a content
// offset (`fetch_start + wire_delta`) would overshoot by the proof overhead,
// silently skipping ~one proof's worth of delivered content from billing — a
// sub-1% under-pay that a content-only cost floor cannot see (the paid proof
// inflates any honest settle above it), which is why the floor below is the
// whole-blob WIRE cost.

// The pool's single working deposit — the amount it opens at and every reactive
// top-up restores its remaining balance back toward. Sized against two competing
// bounds of the shared-pool serve path:
//
//   * The node's pre-serve floor-M reserve (`pool_remaining_covers_window`,
//     #1516) refuses to open a stream unless the pool's remaining minus the
//     refundable floor `M` (default 1 USDC) covers the reserved CREDIT WINDOW.
//     The gate prices a cold, unbounded request at `paid = 0` (ADR 003
//     §Credit window, #1669), which the ramp collapses to its floor — exactly
//     ONE chunk (`decdn_protocol::client::CHUNK_BYTES`, 1 MiB); at
//     `MULTI_RATE_PER_MB` that floor costs 1 * 2_000_000 = 2 USDC, so the
//     deposit must clear 2 USDC + M = 3 USDC just to open. 18 USDC clears this
//     with room to spare.
//   * Yet it must stay below the whole ~9 MiB blob's cost (~19 USDC) so a later
//     voucher exhausts it mid-stream and the reactive top-up fires.
//
// 18 USDC threads both: the stream opens (18 − 1 = 17 ≥ 2), nine whole chunks
// are delivered and metered (cumulative 18 USDC), and the partial tail — the
// blob is just over nine chunks — is the one that genuinely exhausts the
// deposit. It also stays clear of the reuse-time
// low-water auto-refill (#1103): this test pre-opens and pre-records the pool,
// and on the CLI's one invocation the pool's full 18 USDC remaining sits above
// the low-water trigger (working / LOW_WATER_DIVISOR = 3.6 USDC), so no
// proactive refill pre-empts the REACTIVE (mid-stream) top-up under test.
//
// A daemon restart between the pool's on-chain open and this test's later
// on-chain `topUp` was observed to make the daemon's settlement watcher stop
// applying `PoolToppedUp` events to its tracked pool state — the admin API
// kept reporting the pre-top-up deposit indefinitely, well past any poll
// interval, causing the resumed voucher to be rejected forever. That looks
// like a real, separate bug in the watcher/restart interaction, out of scope
// for this fix; avoiding any daemon restart in this test sidesteps it entirely.
const MULTI_WORKING_DEPOSIT_MICRO_USDC: u64 = 18_000_000;
const MULTI_RATE_PER_MB: u64 = 2_000_000; // 2 USDC/MB

// A single-invocation reactive MID-STREAM top-up extends a fetch past its
// opening deposit: `cli/src/commands/fetch.rs::open_or_reuse_pool` signs the
// self-capability with `spending_cap = U256::MAX`, so the on-chain cap
// `PaymentPool._registerCapability` fixes at first redemption never binds. The
// pool deposit — not the capability cap — is the real spending bound, and
// `redeem` pays `min(desired, cap-spent, remaining)` against whatever the
// deposit is at redemption time, including a `topUp` that lands mid-stream.
// This exercises the multi-interval case: several intervals deliver against
// the opening deposit before a top-up-funded voucher crosses it.
#[tokio::test(flavor = "multi_thread")]
async fn fetch_topup_after_several_delivered_intervals_does_not_double_pay() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_multi_interval_topup()))
        .await
        .context("multi-interval reactive top-up e2e exceeded the overall timeout")??;
    Ok(())
}

/// A deterministic blob spanning just over nine whole `CHUNK_BYTES` chunks, so
/// nine full chunks are delivered and metered before a small partial tail
/// exhausts `MULTI_WORKING_DEPOSIT_MICRO_USDC`.
fn make_multi_interval_blob() -> Vec<u8> {
    let mut v = vec![0u8; 9 * 1024 * 1024 + 777];
    let mut x: u32 = 0x2468_ac13;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's pool/\
              deposit state, so decomposing it would thread state through helpers without reducing \
              the journey's length or making it easier to follow"
)]
async fn run_multi_interval_topup() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // A blob spanning just over nine whole chunks, so nine full chunks get
    // delivered and metered before a small partial tail exhausts the deposit.
    let blob = make_multi_interval_blob();
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );
    node.set_rate_per_mb(MULTI_RATE_PER_MB).await?;

    let client_dir = tempfile::tempdir().context("client tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        client_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod client dir 0o700")?;
    eth_identity::generate_and_persist(client_dir.path(), TOPUP_KEYSTORE_PASSWORD, false)
        .context("generate buyer keystore")?;
    let keystore = eth_identity::keystore_path(client_dir.path());
    let buyer =
        eth_identity::load_signer(&keystore, TOPUP_KEYSTORE_PASSWORD).context("load buyer")?;
    let buyer_addr = buyer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(MULTI_WORKING_DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    // Pre-open the pool ourselves and wait for the node to observe it, rather
    // than letting the CLI's own `open_or_reuse` race the node's chain watcher.
    // A blind retry loop that re-invokes the WHOLE `decdn fetch` process on ANY
    // failure — including ones unrelated to the race — would let a second
    // invocation resume from whatever the first one flushed via the
    // function-ENTRY `resume_offset(existing_partial_len(...))` path (unrelated
    // to this fix, and pre-existing), which would corrupt the very cost
    // measurement this test exists to take. Pre-clearing the race keeps this
    // test to exactly ONE `decdn fetch` invocation, so the reactive top-up
    // branch is the only thing that can move the byte offset.
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
    ensure_allowance(
        &chain.provider_for(&buyer),
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_pool,
        None,
    )
    .await
    .context("approve PaymentPool")?;
    let pool = PaymentPool::new(chain.addrs().payment_pool, chain.provider_for(&buyer));
    // Escrowed as configured — no on-chain floor to clamp up to, only a
    // non-zero requirement (`openPool` reverts `ZeroAmount`).
    let working_deposit = U256::from(MULTI_WORKING_DEPOSIT_MICRO_USDC);
    let opened = open_pool(
        &pool,
        Arc::new(buyer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        working_deposit,
    )
    .await
    .context("open buyer pool")?;
    // The buyer self-signs, so the lane's signer is its own address.
    let lane = LaneKey {
        pool_id: opened.state.pool_id,
        signer: buyer_addr,
        provider: node.operator_addr(),
    };
    {
        // Scoped: the CLI subprocess below opens its OWN handle on the same
        // redb file, and the store enforces single-writer access — this
        // handle must be dropped before spawning `decdn fetch`.
        let store = RedbBuyerPoolStore::open(client_dir.path()).context("open buyer store")?;
        store.record(&opened.state).context("record pool")?;
    }
    // No node-side readiness wait: a pool has no on-chain-observed lane until its
    // first voucher lands, and the node serves it as soon as its `getPool` view
    // resolves the deposit — which is already on-chain (the `open_pool` receipt is
    // awaited above). So the single fetch below can proceed straight away.

    let out = client_dir.path().join("blob.bin");
    let args = topup_fetch_argv_with_deposits(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
        MULTI_WORKING_DEPOSIT_MICRO_USDC,
    );

    let before = billed_bytes(client_dir.path(), node.operator_addr())?;
    anyhow::ensure!(
        before == 0,
        "no bytes should be billed before the first fetch"
    );

    // Exactly one *delivering* invocation — see the comment above on why this
    // test avoids a blind cross-invocation retry.
    // The one exception is the node's serve-path readiness race: right after
    // `openPool` mines, the node can still answer `NotFound` for the brief window
    // before its `getPool` view resolves the new pool (there is no pool-open event
    // to wait on). A
    // pure readiness `NotFound` delivers no byte and writes no partial, so
    // re-invoking is safe and cannot double-pay; the moment any byte lands we stop
    // retrying, so the reactive top-up branch remains the only thing that can move
    // the byte offset within the one delivering run.
    let partial = client_dir.path().join("blob.bin.partial");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = tokio::process::Command::from(decdn_command(
            client_dir.path(),
            TOPUP_KEYSTORE_PASSWORD,
        )?)
        .arg("fetch")
        .args(&args)
        .output()
        .await
        .context("spawn decdn fetch")?;
        if output.status.success() {
            break;
        }
        // Anything already on disk means the run began delivering; a re-invocation
        // would resume from that flushed prefix and corrupt the cost measurement,
        // so surface the failure rather than retry.
        let progressed = out.exists() || std::fs::metadata(&partial).is_ok_and(|m| m.len() > 0);
        anyhow::ensure!(
            !progressed && tokio::time::Instant::now() < deadline,
            "decdn fetch failed (delivered bytes on disk = {progressed}): {}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let got = std::fs::read(&out).context("read output")?;
    anyhow::ensure!(
        got == blob,
        "the fetch must still complete successfully after the reactive top-up: got {} bytes, \
         expected {}",
        got.len(),
        blob.len()
    );
    anyhow::ensure!(
        !partial.exists(),
        "the .partial scratch file must be promoted away, not left beside --output: {}",
        partial.display()
    );

    // The money-correctness assertion: the lane's persisted cumulative voucher
    // watermark (`last_amount` — the off-chain figure a `redeem` would later
    // claim on-chain; the pool is never closed in this test, so there is no
    // on-chain redeemed amount to read yet) must be the blob's real WIRE cost —
    // delivered ONCE — never `blob_cost + already_delivered_prefix` (double-pay)
    // and never below the whole-blob wire cost (under-pay).
    //
    // Two reference costs, both via the node's/`next_voucher`'s `ceil(bytes *
    // rate / MB)` voucher-pricing formula:
    //
    //  * `true_cost` — over the blob's CONTENT bytes. This is the spec figure and
    //    an absolute floor, but NOT a tight one: vouchers actually pay for WIRE
    //    bytes (content + interleaved bao proof, ADR 038), so every honest settle
    //    sits ABOVE `true_cost` by the paid proof. A content-only floor therefore
    //    cannot see the wire-vs-content under-pay this test guards — the skipped
    //    sliver (~one proof's worth) is smaller than the proof overhead that
    //    inflates the settle above `true_cost`. It is kept only for context.
    //  * `wire_floor` — over the blob's exact bao WIRE bytes (`bao_encoded_size`,
    //    the identical tree walk the serve encoder and the pull's
    //    `expected_wire_bytes` use). A correct fetch pays for every one of these
    //    bytes at least once, so `wire_floor` is the TIGHT no-under-pay gate: a
    //    content-offset resume skips delivered content and settles strictly below it.
    let blob_len = u64::try_from(blob.len()).context("blob length as u64")?;
    let ceil_cost = |bytes: u64| {
        U256::from(bytes)
            .saturating_mul(U256::from(MULTI_RATE_PER_MB))
            .div_ceil(U256::from(MB_BYTES))
    };
    let true_cost = ceil_cost(blob_len);
    // Exact whole-blob wire size: content + every 64-byte bao proof node, in
    // pre-order (ADR 038). `align_range(0, 0, blob_len)` is the whole-blob range;
    // its `wire_len()` is `bao_encoded_size` over the real block-size tree.
    let whole_wire = align_range(0, 0, blob_len)
        .context("align whole blob")?
        .wire_len();
    let wire_floor = ceil_cost(whole_wire);
    // One 16 KiB chunk group's cost. The conservative resume snaps the paid
    // frontier DOWN to a group boundary, so the resumed leg re-fetches STRICTLY
    // LESS than one group of already-paid content; two groups of headroom above
    // `wire_floor` also covers the resumed leg's own left-boundary proof hashes
    // (~log2(groups) × 64 B) and the handful of per-voucher `ceil` roundings —
    // and is still far below a single re-paid chunk, which is what a genuine
    // double-pay would add.
    let one_group_cost = ceil_cost(decdn_bao_range::CHUNK_GROUP_BYTES);
    let ceiling = wire_floor.saturating_add(one_group_cost.saturating_mul(U256::from(2u64)));

    let store = RedbBuyerPoolStore::open(client_dir.path()).context("open buyer store")?;
    let persisted = store
        .get_by_owner(buyer_addr)
        .context("read persisted pool")?
        .ok_or_else(|| anyhow::anyhow!("buyer pool not recorded after fetch"))?;
    let settled_amount = persisted
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("lane watermark not recorded after fetch"))?
        .last_amount;

    // Upper bound: no double-pay. At most one chunk group of re-fetched paid
    // content plus small proof/rounding slack above the whole-blob wire cost.
    anyhow::ensure!(
        settled_amount <= ceiling,
        "the lane must not have double-paid the prefix delivered before the top-up: \
         settled {settled_amount} µUSDC, but the whole-blob WIRE cost at {MULTI_RATE_PER_MB} µUSDC/MB is \
         {wire_floor} µUSDC (content-only cost {true_cost}); tight ceiling {ceiling} allows \
         under one re-fetched chunk group — a double-pay would settle a full interval higher"
    );
    // Lower bound: no under-pay. The whole blob's wire bytes were paid at least
    // once. The wire-vs-content bug skipped ~one proof's worth of delivered
    // content and would settle BELOW `wire_floor` (yet still above the coarse
    // content-only `true_cost`, which is why the floor must be `wire_floor`).
    anyhow::ensure!(
        settled_amount >= wire_floor,
        "the lane under-paid: settled {settled_amount} µUSDC, below the whole-blob WIRE cost of \
         {wire_floor} µUSDC (content-only {true_cost}) — resuming past the true content paid \
         frontier would skip billing the delivered-but-unpaid tail exactly like this"
    );

    drop(node);
    Ok(())
}

// ---- Two reactive top-ups in ONE fetch: the paid-frontier baselines are per-leg ----
//
// The multi-interval test above drives exactly ONE top-up, sized so the working
// deposit finishes the blob. That leaves the loop's most fragile invariant
// untested: `content_paid_frontier` inverts the wire cost of ONE contiguous
// delivery starting at `fetch_start_offset`, but `ledger.committed().bytes` is
// CHANNEL-cumulative and keeps climbing across every leg of the fetch.
//
// If the two baselines (`fetch_start_offset` / `fetch_start_committed_bytes`) are
// captured once per FETCH rather than re-anchored per LEG, the second top-up feeds
// the helper the SUM of two independent bao range encodings — which re-bills the
// first leg's span and its re-sent root->offset proof path — against a start offset
// that is still the fetch's original one. The inflated budget maps to a frontier
// PAST the true paid one: content skipped unbilled, `byte_offset` beyond the
// verified on-disk prefix, `set_len` zero-extending the partial, and the whole-file
// hash check failing a fetch that was already paid for.
//
// Sizing, at `TWO_TOPUP_RATE_PER_MB` and the protocol's fixed `CHUNK_BYTES`
// quantum, so exactly two reactive top-ups are needed:
//
//   * The node's pre-serve deposit gate (#1518) refuses to serve unless headroom
//     covers the reserved credit window. For a cold, unbounded request the ramp
//     (ADR 003 §Credit window, #1669) prices at `paid = 0`, its floor — one
//     chunk, 1 * 2_000_000 = 2_000_000 µUSDC at the quoted rate. So the deposit
//     must clear that floor (plus `M`), or the resumed open after a top-up is
//     refused instead of served. 16M clears it with headroom — the number below
//     is driven by the delivery arithmetic, not by this floor.
//   * The claim accumulates 2_000_000 µUSDC per whole chunk, so a 16_000_000
//     deposit buys eight chunks before the ninth exhausts it. Each top-up
//     restores headroom to the full 16_000_000 working deposit, buying eight
//     more. The quantum sets only the granularity of that walk; what decides the
//     top-up COUNT is the total: a ~20 MiB blob costs ~40_300_000 µUSDC of wire,
//     which lands strictly between the deposit plus one top-up (32M — so a
//     second top-up IS required) and the deposit plus two top-ups (48M — so two
//     suffice, inside the `MAX_TOPUP_ATTEMPTS` budget of 3).
//
// The money bound is the same two-sided WIRE band the single-top-up test uses, and
// it is the point of the test: across two top-ups the blob's wire bytes must be
// paid for exactly once. The ceiling allows a little more slack here than the
// single-top-up case because there are two conservative group-snapped resumes, each
// re-fetching strictly under one chunk group, plus each resumed leg's own
// left-boundary proof hashes.

const TWO_TOPUP_RATE_PER_MB: u64 = 2_000_000; // 2 USDC/MB, as above
// Must clear the ramp-floor one-chunk credit window at the rate above
// (2_000_000 µUSDC) — sized well above that floor for the delivery arithmetic
// explained above.
const TWO_TOPUP_WORKING_MICRO_USDC: u64 = 16_000_000;
/// Just over 20 MiB: costs more than the deposit plus one top-up (forcing a
/// SECOND top-up) and less than the deposit plus two top-ups (so two are enough).
const TWO_TOPUP_BLOB_BYTES: usize = 20 * 1024 * 1024 + 4113;

/// IGNORED — a blob this far past the working deposit does not complete end to end.
///
/// The per-signer floor does not block a reactive-top-up resume: it is a refilling
/// abandonment bucket (ADR 003 §Pool solvency), so a single-signer resume admits at
/// the floor gate rather than tripping a per-signer cap. What remains are separate,
/// non-floor blockers this test still trips: the resumed
/// leg can open before the first voucher materializes its lane in the node's store
/// (`stream request on unknown lane; refusing pre-serve`), and the deposit/window
/// sizing no longer reliably forces exactly two top-ups. Un-ignore once the
/// lane-materialization race and the sizing are addressed; the driver below already
/// asserts the two-top-up byte accounting.
#[ignore = "resumed-leg lane-materialization race + top-up sizing; floor lockout resolved — see the doc comment"]
#[tokio::test(flavor = "multi_thread")]
async fn fetch_across_two_reactive_topups_pays_each_wire_byte_exactly_once() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_two_topup_fetch()))
        .await
        .context("two-top-up reactive e2e exceeded the overall timeout")??;
    Ok(())
}

fn make_two_topup_blob() -> Vec<u8> {
    let mut v = vec![0u8; TWO_TOPUP_BLOB_BYTES];
    let mut x: u32 = 0x1357_9bdf;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential end-to-end journey: each step depends on the previous step's pool/\
              deposit state, so decomposing it would thread state through helpers without reducing \
              the journey's length or making it easier to follow"
)]
async fn run_two_topup_fetch() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = make_two_topup_blob();
    let blob_hash = Hash::new(&blob);
    let (node, hash) = NodeFixture::launch(&chain, "US", &blob).await?;
    anyhow::ensure!(
        hash == blob_hash,
        "seeded blob hash mismatch: {hash} vs {blob_hash}"
    );
    node.set_rate_per_mb(TWO_TOPUP_RATE_PER_MB).await?;

    let client_dir = tempfile::tempdir().context("client tempdir")?;
    #[cfg(unix)]
    std::fs::set_permissions(
        client_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod client dir 0o700")?;
    eth_identity::generate_and_persist(client_dir.path(), TOPUP_KEYSTORE_PASSWORD, false)
        .context("generate buyer keystore")?;
    let keystore = eth_identity::keystore_path(client_dir.path());
    let buyer =
        eth_identity::load_signer(&keystore, TOPUP_KEYSTORE_PASSWORD).context("load buyer")?;
    let buyer_addr = buyer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    // Enough for the open plus both top-ups, with headroom.
    chain
        .mint_usdc(buyer_addr, U256::from(5 * TWO_TOPUP_WORKING_MICRO_USDC))
        .await
        .context("mint buyer USDC")?;

    // Pre-open and pre-record the pool, and never restart the daemon — same
    // reasoning as the multi-interval test above: this keeps the journey to
    // exactly ONE `decdn fetch` invocation, so the reactive top-up branch is the
    // only thing that can move the byte offset, and the settle measurement below
    // is not corrupted by a cross-invocation resume.
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
    ensure_allowance(
        &chain.provider_for(&buyer),
        chain.usdc(),
        buyer_addr,
        chain.addrs().payment_pool,
        None,
    )
    .await
    .context("approve PaymentPool")?;
    let pool = PaymentPool::new(chain.addrs().payment_pool, chain.provider_for(&buyer));
    let opened = open_pool(
        &pool,
        Arc::new(buyer.clone()),
        &voucher_dom,
        chain.usdc(),
        buyer_addr,
        U256::from(TWO_TOPUP_WORKING_MICRO_USDC),
    )
    .await
    .context("open buyer pool")?;
    // The buyer self-signs, so the lane's signer is its own address.
    let lane = LaneKey {
        pool_id: opened.state.pool_id,
        signer: buyer_addr,
        provider: node.operator_addr(),
    };
    {
        // Scoped: the CLI subprocess opens its own handle on the same redb file
        // and the store is single-writer.
        let store = RedbBuyerPoolStore::open(client_dir.path()).context("open buyer store")?;
        store.record(&opened.state).context("record pool")?;
    }
    // No node-side readiness wait: the node serves the pool as soon as its
    // `getPool` view resolves the on-chain deposit (the `open_pool` receipt is
    // awaited above); a lane only appears in the node's store on the first voucher.

    let out = client_dir.path().join("blob.bin");
    let args = topup_fetch_argv_with_deposits(
        &chain,
        &node,
        &blob_hash,
        client_dir.path(),
        &keystore,
        &out,
        TWO_TOPUP_WORKING_MICRO_USDC,
    );

    let output =
        tokio::process::Command::from(decdn_command(client_dir.path(), TOPUP_KEYSTORE_PASSWORD)?)
            .arg("fetch")
            .args(&args)
            .output()
            .await
            .context("spawn decdn fetch")?;
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    anyhow::ensure!(output.status.success(), "decdn fetch failed: {stderr}");

    // The fetch must have taken the reactive branch TWICE. Without this the test
    // could pass having driven the single-top-up path the test above already
    // covers, and the per-leg baseline invariant would go unexercised.
    let topups = stderr.matches("topped up").count();
    anyhow::ensure!(
        topups == 2,
        "the sizing must force exactly two reactive top-ups (saw {topups}); \
         re-check the deposit/rate/blob arithmetic against the pre-serve credit-window \
         gate. stderr:\n{stderr}"
    );

    // Byte-exact delivery. This is where a frontier that ran PAST the true paid
    // one shows up: `set_len` would have zero-extended the partial over the
    // skipped span, and the whole-file BLAKE3 check would reject the result.
    let got = std::fs::read(&out).context("read output")?;
    anyhow::ensure!(
        got == blob,
        "the blob must be byte-exact after two reactive top-ups: got {} bytes, expected {}",
        got.len(),
        blob.len()
    );
    let partial = client_dir.path().join("blob.bin.partial");
    anyhow::ensure!(
        !partial.exists(),
        "the .partial scratch file must be promoted away, not left beside --output: {}",
        partial.display()
    );

    // The money bound, two-sided over WIRE bytes — see the multi-interval test
    // above for why the floor must be the wire cost and not the content-only one.
    let blob_len = u64::try_from(blob.len()).context("blob length as u64")?;
    let ceil_cost = |bytes: u64| {
        U256::from(bytes)
            .saturating_mul(U256::from(TWO_TOPUP_RATE_PER_MB))
            .div_ceil(U256::from(MB_BYTES))
    };
    let whole_wire = align_range(0, 0, blob_len)
        .context("align whole blob")?
        .wire_len();
    let wire_floor = ceil_cost(whole_wire);
    // Two conservative resumes, each re-fetching strictly under one 16 KiB chunk
    // group, plus each resumed leg's left-boundary proof path and the per-proof
    // `ceil` roundings. Four groups of headroom covers all of it and is still far
    // below the 2_000_000 a single re-paid chunk would add.
    let one_group_cost = ceil_cost(decdn_bao_range::CHUNK_GROUP_BYTES);
    let ceiling = wire_floor.saturating_add(one_group_cost.saturating_mul(U256::from(4u64)));

    let store = RedbBuyerPoolStore::open(client_dir.path()).context("open buyer store")?;
    let persisted = store
        .get_by_owner(buyer_addr)
        .context("read persisted pool")?
        .ok_or_else(|| anyhow::anyhow!("buyer pool not recorded after fetch"))?;
    let settled_amount = persisted
        .lane_progress(lane)
        .ok_or_else(|| anyhow::anyhow!("lane watermark not recorded after fetch"))?
        .last_amount;

    anyhow::ensure!(
        settled_amount >= wire_floor,
        "under-pay across two top-ups: settled {settled_amount} µUSDC, below the whole-blob WIRE cost of \
         {wire_floor} µUSDC. A second-leg paid frontier derived from a stale baseline \
         overshoots the true one and skips billing exactly this way"
    );
    anyhow::ensure!(
        settled_amount <= ceiling,
        "double-pay across two top-ups: settled {settled_amount} µUSDC against a whole-blob WIRE cost of \
         {wire_floor} µUSDC (ceiling {ceiling}); a re-paid chunk would add 2000000"
    );

    // Both top-ups landed on-chain and are reflected locally. Each one restores
    // headroom to the working deposit, so the escrow ends strictly above what one
    // top-up alone could have reached (open plus one top-up = 2× the working
    // deposit).
    let one_topup_ceiling = U256::from(2 * TWO_TOPUP_WORKING_MICRO_USDC);
    anyhow::ensure!(
        persisted.deposit > one_topup_ceiling,
        "two top-ups must escrow more than a single top-up could ({one_topup_ceiling}); got {}",
        persisted.deposit
    );
    // The escrow must still cover everything vouchered — the fetch completed, so
    // the final voucher was within deposit.
    anyhow::ensure!(
        persisted.deposit >= settled_amount,
        "escrow {} must cover the settled amount {}",
        persisted.deposit,
        settled_amount
    );
    let onchain = pool
        .getPool(opened.state.pool_id)
        .call()
        .await
        .context("read on-chain pool")?
        .deposit;
    anyhow::ensure!(
        U256::from(onchain) == persisted.deposit,
        "the persisted deposit must match the chain after two top-ups: local {} vs chain \
         {onchain}",
        persisted.deposit
    );

    drop(node);
    Ok(())
}
