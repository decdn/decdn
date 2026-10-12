use super::*;
use crate::session::ChainAdoption;
use alloy::primitives::B256;
use decdn_incentive::buyer_pool::{BuyerLaneProgress, BuyerPoolState};
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{Deployment, bind_node_id_domain};

fn ctx_with(binding: Option<decdn_protocol::client::ClientBinding>) -> PoolContext {
    ctx_with_deposit(binding, U256::ZERO)
}

fn ctx_with_deposit(
    binding: Option<decdn_protocol::client::ClientBinding>,
    deposit: U256,
) -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        provider: Address::ZERO,
        deposit,
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: bind_node_id_domain(1, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: binding,
        capability: None,
    }
}

/// A registry-backed `lane_ledger` call for the same lane must return the
/// SAME ledger `Arc` on a second call (no fresh mint per fetch/chunk) — the
/// invariant `LaneLedgers::get_or_insert` exists to guarantee. A `None`
/// registry (standalone `decdn fetch`) must mint a distinct ledger every
/// call.
#[test]
fn lane_ledger_shares_one_arc_through_the_registry_but_not_without_one() {
    let lane = LaneKey {
        pool_id: B256::ZERO,
        signer: Address::ZERO,
        provider: Address::from([7u8; 20]),
    };
    let registry = LaneLedgers::new();
    let (ledger_a, ctx_a) = lane_ledger(Some(&registry), lane, ctx_with(None));
    let (ledger_b, ctx_b) = lane_ledger(Some(&registry), lane, ctx_with(None));
    assert!(Arc::ptr_eq(&ledger_a, &ledger_b));
    assert!(Arc::ptr_eq(&ctx_a, &ctx_b));

    let (ledger_none_a, _) = lane_ledger(None, lane, ctx_with(None));
    let (ledger_none_b, _) = lane_ledger(None, lane, ctx_with(None));
    assert!(!Arc::ptr_eq(&ledger_none_a, &ledger_none_b));
}

/// A second `lane_ledger` touch of an already-registered lane must reconcile
/// the shared handle's `ctx.deposit` UPWARD to a freshly-read higher deposit
/// (a low-water top-up `open_or_reuse_pool` performed between the two
/// touches) rather than silently discarding it. The pool-wide spent/credit
/// gate reads this shared value, so a stale low deposit would refuse
/// (`PoolExhausted`) prematurely even though the on-chain balance grew.
#[test]
fn lane_ledger_reconciles_shared_deposit_upward_on_reuse() {
    let lane = LaneKey {
        pool_id: B256::ZERO,
        signer: Address::ZERO,
        provider: Address::from([9u8; 20]),
    };
    let registry = LaneLedgers::new();
    let (_, ctx_a) = lane_ledger(
        Some(&registry),
        lane,
        ctx_with_deposit(None, U256::from(100)),
    );
    assert_eq!(
        ctx_a.lock().unwrap_or_else(PoisonError::into_inner).deposit,
        U256::from(100)
    );

    // A later touch on the same lane with a higher freshly-read deposit
    // (simulating a top-up) must raise the shared handle, not discard it.
    let (_, ctx_b) = lane_ledger(
        Some(&registry),
        lane,
        ctx_with_deposit(None, U256::from(250)),
    );
    assert!(Arc::ptr_eq(&ctx_a, &ctx_b));
    assert_eq!(
        ctx_b.lock().unwrap_or_else(PoisonError::into_inner).deposit,
        U256::from(250)
    );

    // A lower freshly-read deposit than the shared handle's current value
    // must never lower it (deposit only ever grows via top-ups).
    let (_, ctx_c) = lane_ledger(
        Some(&registry),
        lane,
        ctx_with_deposit(None, U256::from(10)),
    );
    assert_eq!(
        ctx_c.lock().unwrap_or_else(PoisonError::into_inner).deposit,
        U256::from(250)
    );
}

fn rebase_store_fixture() -> anyhow::Result<(
    tempfile::TempDir,
    RedbBuyerPoolStore,
    Address,
    PoolId,
    LaneKey,
)> {
    let dir = tempfile::tempdir()?;
    // A fresh subpath so the store's `ensure_data_dir` creates it at 0o700.
    let store = RedbBuyerPoolStore::open(&dir.path().join("data"))?;
    let owner = Address::repeat_byte(0x0A);
    let pool_id = PoolId::repeat_byte(0x01);
    let lane = LaneKey {
        pool_id,
        signer: owner,
        provider: Address::repeat_byte(0xB0),
    };
    let mut state = BuyerPoolState::new(
        pool_id,
        Deployment {
            chain_id: 421_614,
            payment_pool: Address::repeat_byte(0x7B),
        },
        owner,
        Address::repeat_byte(0x7C),
        U256::from(1_000u64),
    );
    state
        .advance_lane(lane, U256::from(500u64), U256::from(90u64))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    store.record(&state)?;
    Ok((dir, store, owner, pool_id, lane))
}

fn lane_record(
    store: &dyn BuyerPoolStore,
    pool_id: PoolId,
    lane: LaneKey,
) -> anyhow::Result<BuyerLaneProgress> {
    store
        .get_by_pool_id(pool_id)?
        .and_then(|s| s.lane_progress(lane))
        .ok_or_else(|| anyhow::anyhow!("lane record missing"))
}

/// Two bundle entries on one provider share one connection: each entry
/// builds its own deps and lane, and both lanes open their streams on the
/// command's connection to the node.
#[allow(
    clippy::too_many_lines,
    reason = "one fixture: a stream-dropping node, the lane deps, two entries"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_entries_on_one_provider_share_one_connection() -> anyhow::Result<()> {
    use crate::source::BlobSource as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    async fn loopback(alpns: Vec<Vec<u8>>) -> anyhow::Result<(Endpoint, EndpointAddr)> {
        let ep = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(alpns)
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))?
            .bind()
            .await?;
        let socket = ep
            .bound_sockets()
            .into_iter()
            .find(std::net::SocketAddr::is_ipv4)
            .ok_or_else(|| anyhow::anyhow!("no IPv4 socket"))?;
        let addr = EndpointAddr::new(ep.id()).with_ip_addr(socket);
        Ok((ep, addr))
    }

    // A node that accepts every connection and drops every stream it is
    // sent, so each open fails on a connection that stays up.
    let (node, node_addr) = loopback(vec![decdn_protocol::ALPN_CLIENT.to_vec()]).await?;
    let accepted = Arc::new(AtomicU64::new(0));
    let (accept_node, count) = (node.clone(), Arc::clone(&accepted));
    tokio::spawn(async move {
        while let Some(incoming) = accept_node.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                while let Ok((send, recv)) = conn.accept_bi().await {
                    drop((send, recv));
                }
            });
        }
    });

    let (ep, _) = loopback(Vec::new()).await?;
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn BuyerPoolStore> =
        Arc::new(RedbBuyerPoolStore::open(&dir.path().join("data"))?);
    let rpc = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new());
    let contract = PaymentPool::new(Address::repeat_byte(0x33), rpc.clone());
    let chain = ChainConfig {
        rpc_url: "http://config:8545".to_string(),
        chain_id: 1,
        payment_pool: Address::repeat_byte(0x33),
        slash_judge: Address::repeat_byte(0x44),
        capacity_bond: None,
    };
    let site = PullSite {
        data_dir: dir.path().to_path_buf(),
        region: None,
        region_allowlist: Vec::new(),
    };
    let slash_dom = decdn_incentive::slash_judge_domain(1, Address::repeat_byte(0x44));
    let connections = Connections::new(ep.clone());
    let writes = OrderedWrites::default();
    let funding = RunFunding::default();
    let entry_deps = || -> anyhow::Result<LaneDeps<'_, _>> {
        Ok(LaneDeps {
            marks: None,
            endpoint: &ep,
            store: &store,
            contract: &contract,
            rpc: &rpc,
            slash_dom: &slash_dom,
            self_address: Address::repeat_byte(0x0A),
            token: Address::repeat_byte(0x22),
            chain: &chain,
            site: &site,
            deposit: DepositPolicy {
                working_deposit: U256::from(1_000_000u64),
                max_approve: false,
                adoption: ChainAdoption::Refused,
            },
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            max_rate_per_mb: 0,
            max_blob_bytes: 0,
            deadlines: PullDeadlines::new(Duration::from_secs(5), Duration::from_secs(5), 0)?,
            connections: &connections,
            writes: &writes,
            funding: &funding,
        })
    };
    let provider = Address::repeat_byte(0xB0);
    let ctx = PoolContext {
        pool_id: B256::ZERO,
        provider,
        deposit: U256::from(1_000_000u64),
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: decdn_incentive::voucher_domain(1, Address::repeat_byte(0x33)),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    };
    let ledger = Arc::new(ctx.new_ledger());
    let ctx = Arc::new(Mutex::new(ctx));
    let range = decdn_bao_range::align_range(0, 0, 1024)?;
    for entry in [[0x01u8; 32], [0x02u8; 32]] {
        let deps = entry_deps()?;
        let source = lane_source(
            &deps,
            node_addr.clone(),
            Arc::clone(&ctx),
            Arc::clone(&ledger),
            provider,
        );
        assert!(
            source.open(entry, range.clone()).await.is_err(),
            "the node drops every stream"
        );
    }

    assert_eq!(connections.dials(), 1, "both entries share one dial");
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn settle_on_drop_runs_only_when_dropped_armed() {
    let settled = std::cell::Cell::new(0);
    drop(SettleOnDrop::new(|| settled.set(settled.get() + 1)));
    assert_eq!(settled.get(), 1, "a dropped drive settles");
    SettleOnDrop::new(|| settled.set(settled.get() + 1)).disarm();
    assert_eq!(settled.get(), 1, "a returned drive settles on its own path");
}

/// `persist_watermark` overwrites the lane record down to a rebase anchor and
/// advances it to the ledger's totals, even below what the record holds. A
/// persist without an anchor is a monotone advance again, so a lower totals
/// snapshot cannot move the record down.
#[test]
fn persist_watermark_overwrites_once_on_a_rebase() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let anchor = Cumulative {
        bytes: U256::from(100u64),
        amount: U256::from(60u64),
    };
    let totals = Cumulative {
        bytes: U256::from(200u64),
        amount: U256::from(70u64),
    };
    let progress = VoucherProgress::from_cumulative(totals, U256::from(90u64))
        .with_rebase_anchor(Some(anchor));
    persist_watermark(&store, owner, pool_id, lane, &progress);
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (totals.bytes, totals.amount)
    );

    let lower = Cumulative {
        bytes: U256::from(150u64),
        amount: U256::from(65u64),
    };
    let progress = VoucherProgress::from_cumulative(lower, U256::ZERO);
    persist_watermark(&store, owner, pool_id, lane, &progress);
    let got = lane_record(&store, pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (totals.bytes, totals.amount),
        "without an anchor the persist is monotone"
    );
    Ok(())
}

/// A lane built twice on one shared ledger commits once per entry; a lane
/// on its own ledger keeps its own write. The one write takes the lowest
/// baseline of the ledger's handles: a sibling's rebase moved the store
/// from 90 to 70 between the two builds, and the ledger settled at 80, so
/// the advance from 70 to 80 still persists.
#[test]
fn duplicate_lane_handles_commit_once_from_the_lowest_baseline() {
    let handle = |ledger: &Arc<PoolLedger>, provider: u8, prior: u64| FaceLaneHandle {
        pool_id: PoolId::repeat_byte(1),
        provider: Address::repeat_byte(provider),
        prior_amount: U256::from(prior),
        ledger: Arc::clone(ledger),
        ctx: Arc::new(Mutex::new(ctx_with(None))),
    };
    let shared = Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(100u64),
        amount: U256::from(80u64),
    }));
    let other = Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(10u64),
        amount: U256::from(7u64),
    }));
    let handles = [
        handle(&shared, 0xA1, 90),
        handle(&other, 0xB2, 0),
        handle(&shared, 0xA1, 70),
    ];
    let writes = face_watermark_writes(Address::repeat_byte(0x5E), &handles);
    let providers: Vec<Address> = writes.iter().map(|(lane, _)| lane.provider).collect();
    assert_eq!(
        providers,
        [Address::repeat_byte(0xA1), Address::repeat_byte(0xB2)]
    );
    assert_eq!(
        writes[0].1.advanced(),
        Some((U256::from(100u64), U256::from(80u64))),
        "the shared lane advances past its lowest baseline"
    );
}

/// Hold `writes` until the returned sender fires, so every write queued
/// meanwhile waits behind the hold.
fn hold(writes: &OrderedWrites) -> std::sync::mpsc::Sender<()> {
    let (release, hold) = std::sync::mpsc::channel::<()>();
    writes.queue(move || {
        let _ = hold.recv();
    });
    release
}

/// One lane's write of `totals` over baseline `prior`, rebased to `anchor`.
fn lane_write(
    lane: LaneKey,
    totals: (u64, u64),
    prior: u64,
    anchor: Option<(u64, u64)>,
) -> LaneWrite {
    let cum = |(bytes, amount): (u64, u64)| Cumulative {
        bytes: U256::from(bytes),
        amount: U256::from(amount),
    };
    (
        lane,
        VoucherProgress::from_cumulative(cum(totals), U256::from(prior))
            .with_rebase_anchor(anchor.map(cum)),
    )
}

/// Two entries settle one lane: the first read takes the ledger's rebase
/// (down to 60, totals 70), the second a newer advance to 80. They land
/// in that order, so the rebase never replaces the newer row.
#[tokio::test]
async fn a_rebase_read_first_never_lands_over_a_newer_advance() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let store: Arc<dyn BuyerPoolStore> = Arc::new(store);
    let writes = OrderedWrites::default();
    let release = hold(&writes);
    let rebase = lane_write(lane, (200, 70), 90, Some((100, 60)));
    drop(queue_watermark_writes(&writes, &store, owner, vec![rebase]));
    let advance = lane_write(lane, (300, 80), 70, None);
    let landed = queue_watermark_writes(&writes, &store, owner, vec![advance]);
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(10), landed).await??;
    let got = lane_record(store.as_ref(), pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (U256::from(300u64), U256::from(80u64))
    );
    Ok(())
}

/// Two entries settle one lane: the first read is an advance to 95 from
/// before the node refused it, the second the rebase down to 60 (totals
/// 70). They land in that order, so the refused watermark never returns.
#[tokio::test]
async fn a_stale_advance_read_first_never_lands_over_a_rebase() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let store: Arc<dyn BuyerPoolStore> = Arc::new(store);
    let writes = OrderedWrites::default();
    let release = hold(&writes);
    let advance = lane_write(lane, (600, 95), 90, None);
    drop(queue_watermark_writes(
        &writes,
        &store,
        owner,
        vec![advance],
    ));
    let rebase = lane_write(lane, (200, 70), 90, Some((100, 60)));
    let landed = queue_watermark_writes(&writes, &store, owner, vec![rebase]);
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(10), landed).await??;
    let got = lane_record(store.as_ref(), pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (U256::from(200u64), U256::from(70u64))
    );
    Ok(())
}

/// A drop guard's settle drops its receiver and returns. The write it
/// queued, behind a slow one, still lands before the runtime finishes
/// dropping: the runtime waits for the blocking pool as it shuts down.
#[test]
fn a_detached_settle_lands_by_runtime_shutdown() -> anyhow::Result<()> {
    let (_dir, store, owner, pool_id, lane) = rebase_store_fixture()?;
    let store: Arc<dyn BuyerPoolStore> = Arc::new(store);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()?;
    runtime.block_on(async {
        let writes = OrderedWrites::default();
        writes.queue(|| std::thread::sleep(Duration::from_millis(100)));
        let advance = lane_write(lane, (600, 95), 90, None);
        drop(queue_watermark_writes(
            &writes,
            &store,
            owner,
            vec![advance],
        ));
    });
    drop(runtime);
    let got = lane_record(store.as_ref(), pool_id, lane)?;
    assert_eq!(
        (got.last_bytes, got.last_amount),
        (U256::from(600u64), U256::from(95u64))
    );
    Ok(())
}

/// The multi-source path carries each lane's own rebase anchor.
#[test]
fn multi_lane_watermarks_carry_the_rebase_anchor() {
    let anchor = Cumulative {
        bytes: U256::from(50u64),
        amount: U256::from(60u64),
    };
    let mut lane = lane_wm(1, 0xA1, 200, 100, 70);
    lane.rebase_anchor = Some(anchor);
    let out = super::multi_lane_watermarks(Address::repeat_byte(0x5E), &[lane]);
    assert_eq!(out[0].1.rebase_anchor(), Some(anchor));
    assert_eq!(out[0].1.totals(), (U256::from(100u64), U256::from(70u64)));
}

// ---- per-lane watermarks on the multi-source path ----

fn lane_wm(pool: u8, provider: u8, prior: u64, bytes: u64, amount: u64) -> LaneWatermark {
    LaneWatermark {
        pool_id: PoolId::repeat_byte(pool),
        provider: Address::repeat_byte(provider),
        prior_amount: U256::from(prior),
        settlement: Cumulative {
            bytes: U256::from(bytes),
            amount: U256::from(amount),
        },
        rebase_anchor: None,
    }
}

/// Each lane's watermark is persisted under ITS OWN `LaneKey` and carries ITS
/// OWN cumulative. Crossing the two — lane A's amount under lane B's key —
/// strands both lanes, and nothing else in the fetch path would notice.
#[test]
fn multi_lane_watermarks_pair_each_lane_with_its_own_key() {
    let signer = Address::repeat_byte(0x5E);
    let lanes = [lane_wm(1, 0xA1, 0, 100, 200), lane_wm(1, 0xB2, 0, 300, 400)];
    let out = super::multi_lane_watermarks(signer, &lanes);
    assert_eq!(out.len(), 2);

    assert_eq!(out[0].0.provider, Address::repeat_byte(0xA1));
    assert_eq!(out[0].0.signer, signer);
    assert_eq!(out[0].0.pool_id, PoolId::repeat_byte(1));
    assert_eq!(
        out[0].1.advanced(),
        Some((U256::from(100u64), U256::from(200u64))),
        "lane A carries lane A's cumulative"
    );

    assert_eq!(out[1].0.provider, Address::repeat_byte(0xB2));
    assert_eq!(
        out[1].1.advanced(),
        Some((U256::from(300u64), U256::from(400u64))),
        "lane B carries lane B's cumulative"
    );
}

/// A multi-source lane settles at its ARMED cumulative, never at `committed`.
/// A cancelled or stalled leg drops its `fill_gap` future wherever it is
/// parked — including inside the voucher exchange `issue` deliberately arms
/// before sending — and that can happen on a SUCCESSFUL fetch. Settling that
/// lane low persists a cumulative below what the node can redeem, and the next
/// fetch on the lane signs a value the upstream already holds: rejected as a
/// regression.
#[test]
fn multi_lane_watermarks_settle_high_even_when_the_fetch_succeeded() {
    let signer = Address::repeat_byte(0x5E);
    // The armed cumulative sits ABOVE what was acked — the dropped-leg
    // shape. Settling at `committed` would persist the lower one.
    let lanes = [lane_wm(1, 0xA1, 0, 300, 400)];
    let out = super::multi_lane_watermarks(signer, &lanes);
    assert_eq!(
        out[0].1.advanced(),
        Some((U256::from(300u64), U256::from(400u64))),
        "the armed (settlement) cumulative is what gets persisted"
    );
}

/// A lane that advanced nothing persists nothing: `advanced()` is `None`, and
/// `persist_watermark` returns early rather than writing a no-op row.
#[test]
fn multi_lane_watermarks_report_no_advance_for_an_untouched_lane() {
    let signer = Address::repeat_byte(0x5E);
    let lanes = [lane_wm(1, 0xA1, 200, 0, 200)];
    let out = super::multi_lane_watermarks(signer, &lanes);
    assert_eq!(
        out[0].1.advanced(),
        None,
        "a lane at its prior amount has not advanced"
    );
}
