use super::{PeerEvent, holders_from, lane_lease};
use crate::ProvenSize;
use crate::stream_cap::LaneStreamCap;
use alloy::primitives::Address;

/// Two entries of one command file records for the same node through the
/// command's ordered writes: entry A's first open, then entry B's fault.
/// The fault stays stamped, even with both queued behind a slow write.
#[tokio::test]
async fn an_open_and_a_later_fault_from_two_entries_land_in_order() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    crate::PeerStore::open(dir.path()).upsert_identity(
        &crate::discovery::NodeCandidate {
            node_id,
            eth_address: Address::repeat_byte(3),
            region_hint: None,
            multiaddrs: alloy::primitives::Bytes::new(),
        },
        1_000,
    )?;
    let writes = crate::ordered_writes::OrderedWrites::default();
    let (release, hold) = std::sync::mpsc::channel::<()>();
    writes.queue(move || {
        let _ = hold.recv();
    });
    // Each entry opens its own store handle, as each entry's sources do.
    let entry_a = crate::PeerStore::open(dir.path());
    let entry_b = crate::PeerStore::open(dir.path());
    writes.queue(move || {
        PeerEvent::Open {
            node_id,
            rate_per_mb: 4,
        }
        .write(&entry_a);
    });
    writes.queue(move || {
        PeerEvent::Failure {
            node_id,
            at_secs: 2_000,
        }
        .write(&entry_b);
    });
    release.send(())?;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        writes.queue_awaitable(|| ()),
    )
    .await??;
    let landed = crate::PeerStore::open(dir.path())
        .get(&node_id)
        .ok_or_else(|| anyhow::anyhow!("the record is missing"))?;
    assert_eq!(landed.rate_per_mb, Some(4));
    assert_eq!(landed.last_failure_at_secs, Some(2_000));
    Ok(())
}

/// Two entries with crossed providers under a cap of one stream each: X
/// holds A and wants B while Y holds B and wants A. Neither waits: each
/// build fails at once and backs off holding nothing new, and once X
/// finishes and frees A, Y takes it.
#[tokio::test]
async fn crossed_entries_never_wait_for_a_lane_permit() -> anyhow::Result<()> {
    let (a, b) = (Address::repeat_byte(0xA1), Address::repeat_byte(0xB2));
    let cap = LaneStreamCap::new(1);
    let x_holds_a = lane_lease(Some(&cap), a).await?;
    let y_holds_b = lane_lease(Some(&cap), b).await?;

    let crossed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        (
            lane_lease(Some(&cap), b).await,
            lane_lease(Some(&cap), a).await,
        )
    })
    .await
    .map_err(|_| anyhow::anyhow!("a lane build waited for a permit"))?;
    assert!(
        crossed.0.is_err() && crossed.1.is_err(),
        "both permits are held"
    );

    drop(x_holds_a);
    let y_takes_a = lane_lease(Some(&cap), a).await?;
    drop((y_takes_a, y_holds_b));
    assert!(lane_lease(None, a).await.is_ok(), "no cap, no permit");
    Ok(())
}

/// Only the lane that answered the first open is kept; every other lane's
/// lease drops, so its provider's permit frees for sibling entries.
#[tokio::test]
async fn only_the_answering_lane_keeps_its_permit() -> anyhow::Result<()> {
    let (a, b) = (Address::repeat_byte(0xA1), Address::repeat_byte(0xB2));
    let cap = LaneStreamCap::new(1);
    let lanes = vec![
        (a, std::sync::Arc::new(lane_lease(Some(&cap), a).await?)),
        (b, std::sync::Arc::new(lane_lease(Some(&cap), b).await?)),
    ];
    let kept = super::keep_answering(lanes, Some(b));
    assert_eq!(kept.as_ref().map(|(p, _)| *p), Some(b));
    assert!(
        lane_lease(Some(&cap), a).await.is_ok(),
        "A's permit is free"
    );
    assert!(
        lane_lease(Some(&cap), b).await.is_err(),
        "B's lane holds its own"
    );
    drop(kept);
    assert!(super::keep_answering::<()>(Vec::new(), None).is_none());
    Ok(())
}

/// Two nodes of one operator yield one holder: the nearer one by rank.
#[test]
fn one_operator_yields_one_holder_the_nearer_by_rank() {
    let node = |seed: u8| crate::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(0xAA),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let (near, far) = (node(1), node(2));
    let targets = crate::targets::ResolvedTargets {
        candidates: vec![near.clone(), far],
        coverage_by_node: std::collections::HashMap::new(),
        samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
        opened: crate::targets::OpenedSlot::default(),
        pool_hints: std::collections::HashMap::new(),
    };
    let mut nodes = std::collections::HashMap::new();
    let holders = holders_from(&targets, &mut nodes);
    assert_eq!(holders.len(), 1, "one holder per operator");
    assert!(
        holders.iter().all(|h| !h.discovered_holder),
        "no discovery answer reported the blob"
    );
    assert_eq!(
        nodes.get(&Address::repeat_byte(0xAA)).map(|n| n.node_id),
        Some(near.node_id),
        "the nearer node by rank is the operator's holder"
    );
}

#[test]
fn holders_carry_coverage_and_rtt_and_index_their_nodes() {
    let (targets, a, b) = crate::targets::tests_support::two_holder_targets();
    let mut nodes = std::collections::HashMap::new();
    let holders = holders_from(&targets, &mut nodes);
    assert_eq!(holders.len(), 2);
    assert!(
        holders
            .iter()
            .any(|h| h.provider == a.eth_address && h.rtt_ms > 0.0)
    );
    assert!(nodes.contains_key(&b.eth_address));
    let a_holder = holders
        .iter()
        .find(|h| h.provider == a.eth_address)
        .map(|h| h.coverage.clone());
    assert_eq!(
        a_holder,
        Some(targets.coverage_by_node.get(&a.node_id).cloned()),
        "a discovered holder carries its measured coverage"
    );
    assert!(holders.iter().all(|h| h.discovered_holder));
}

/// A holder the order ranks ahead of a nearer one (a cheaper quote in the
/// same RTT band) starts first: its RTT is the floor of every later holder's.
#[test]
fn holders_start_in_failover_order() {
    let node = |seed: u8| crate::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(seed),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let (cheap, near) = (node(1), node(2));
    let targets = crate::targets::ResolvedTargets {
        samples: vec![(cheap.node_id, 15.0, 1), (near.node_id, 5.0, 9)],
        candidates: vec![cheap, near],
        coverage_by_node: std::collections::HashMap::new(),
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
        opened: crate::targets::OpenedSlot::default(),
        pool_hints: std::collections::HashMap::new(),
    };
    let holders = holders_from(&targets, &mut std::collections::HashMap::new());
    let rtts: Vec<f64> = holders.iter().map(|h| h.rtt_ms).collect();
    assert_eq!(rtts, vec![15.0, 15.0]);
}

fn warming(enabled: bool) -> crate::session::ProxyWarmingParams {
    crate::session::ProxyWarmingParams {
        enabled,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

fn late_non_holder(seed: u8, rtt_ms: f64) -> crate::targets::OpenOutcome {
    let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    crate::targets::OpenOutcome::NonHolder(
        crate::discovery::WarmingCandidate {
            node_id,
            eth_address: Address::repeat_byte(seed),
            rtt_ms,
            multiaddrs: alloy::primitives::Bytes::new(),
            payment_pools: None,
        },
        (node_id, rtt_ms, 1),
    )
}

fn late_holder(seed: u8, rtt_ms: f64) -> crate::targets::OpenOutcome {
    let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    crate::targets::OpenOutcome::Holder(
        crate::discovery::DiscoveredHolder {
            candidate: crate::discovery::NodeCandidate {
                node_id,
                eth_address: Address::repeat_byte(seed),
                region_hint: None,
                multiaddrs: alloy::primitives::Bytes::new(),
            },
            rtt_ms,
            rate_per_mb: 1,
            total_bytes: None,
            coverage: decdn_protocol::Coverage::full(4),
            payment_pools: None,
        },
        (node_id, rtt_ms, 1),
    )
}

/// Late non-holders join only as warming proxies against the best holder
/// known so far, and a late holder lowers that best.
#[test]
fn late_join_applies_the_warming_rule() {
    use super::{LateJoin, late_join};
    let mut best = 200.0_f64;
    assert!(matches!(
        late_join(late_non_holder(1, 120.0), &mut best, warming(true)),
        Some(LateJoin::Proxy(_))
    ));
    assert!(late_join(late_non_holder(2, 190.0), &mut best, warming(true)).is_none());
    assert!(late_join(late_non_holder(3, 120.0), &mut best, warming(false)).is_none());
    assert!(matches!(
        late_join(late_holder(4, 100.0), &mut best, warming(true)),
        Some(LateJoin::Holder(_))
    ));
    assert!((best - 100.0).abs() < f64::EPSILON);
    assert!(late_join(late_non_holder(5, 120.0), &mut best, warming(true)).is_none());
}

/// A late node of an operator the set already has never replaces the
/// earlier node; a new operator's node joins and is indexed for
/// `connect`.
#[test]
fn a_late_node_joins_only_for_a_new_operator() -> anyhow::Result<()> {
    use super::{LateJoin, admit_late};
    let near = crate::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[9; 32]).public(),
        eth_address: Address::repeat_byte(4),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let mut nodes = std::collections::HashMap::from([(near.eth_address, near.clone())]);
    let mut best = 200.0_f64;

    let Some(LateJoin::Holder(same_operator)) =
        super::late_join(late_holder(4, 180.0), &mut best, warming(true))
    else {
        anyhow::bail!("a late holder joins");
    };
    assert!(admit_late(&mut nodes, LateJoin::Holder(same_operator)).is_none());
    assert_eq!(nodes.get(&near.eth_address), Some(&near));

    let Some(LateJoin::Holder(other)) =
        super::late_join(late_holder(6, 180.0), &mut best, warming(true))
    else {
        anyhow::bail!("a late holder joins");
    };
    let holder = admit_late(&mut nodes, LateJoin::Holder(other));
    assert!(holder.as_ref().is_some_and(|h| h.discovered_holder));
    assert!(nodes.contains_key(&Address::repeat_byte(6)));
    Ok(())
}

/// Failed outcomes never join.
#[test]
fn late_join_ignores_failed_outcomes() {
    use super::late_join;
    use crate::targets::OpenOutcome;
    let mut best = 400.0;
    for outcome in [
        OpenOutcome::Unreachable,
        OpenOutcome::RateLimited,
        OpenOutcome::ClockSkewed,
        OpenOutcome::Unverifiable,
        OpenOutcome::Unusable,
        OpenOutcome::Declined,
        OpenOutcome::OverCeiling,
    ] {
        assert!(late_join(outcome, &mut best, warming(true)).is_none());
    }
}

/// A rediscovery that ranks another node of an operator first replaces
/// the node the operator's next lane build dials.
#[test]
fn a_rediscovery_refreshes_an_operators_node() {
    let node = |seed: u8| crate::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(0xAA),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let targets = |n: crate::discovery::NodeCandidate| crate::targets::ResolvedTargets {
        samples: vec![(n.node_id, 1.0, 4)],
        candidates: vec![n],
        coverage_by_node: std::collections::HashMap::new(),
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
        opened: crate::targets::OpenedSlot::default(),
        pool_hints: std::collections::HashMap::new(),
    };
    let mut nodes = std::collections::HashMap::new();
    holders_from(&targets(node(1)), &mut nodes);
    holders_from(&targets(node(2)), &mut nodes);
    assert_eq!(
        nodes.get(&Address::repeat_byte(0xAA)).map(|n| n.node_id),
        Some(node(2).node_id)
    );
}

/// The first claim of `targets`, counting the header-only opens it makes.
async fn claim_of(targets: &crate::targets::ResolvedTargets) -> anyhow::Result<(u64, usize)> {
    let opens = std::sync::atomic::AtomicUsize::new(0);
    let mut nodes = std::collections::HashMap::new();
    let holders = holders_from(targets, &mut nodes);
    let claim = super::first_claim_or_open(targets.size_hint, holders, |holders| {
        opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async move {
            Ok(super::FirstClaim {
                total_bytes: ProvenSize::assume_for_tests(7),
                holders,
            })
        }
    })
    .await?;
    Ok((
        claim.total_bytes.get(),
        opens.load(std::sync::atomic::Ordering::SeqCst),
    ))
}

/// Holders whose discovery answer gave a size hint take it as the first claim, with
/// no whole-blob open for the header (#2218).
#[tokio::test]
async fn the_first_claim_comes_from_the_discovery_hint_without_a_whole_blob_open()
-> anyhow::Result<()> {
    let (targets, _, _) = crate::targets::tests_support::two_holder_targets();
    let hint = targets
        .size_hint
        .ok_or_else(|| anyhow::anyhow!("the fixture's discovery answer gives a hint"))?;
    assert_eq!(claim_of(&targets).await?, (hint.get(), 0));
    Ok(())
}

/// A pinned `--node-id` whose discovery answer gave no size, or a pinned bundle entry with
/// a size from neither its manifest nor a discovery answer, opens one pull for the
/// header's size.
#[tokio::test]
async fn a_pinned_node_without_a_hint_opens_for_the_header() -> anyhow::Result<()> {
    let (mut targets, _, _) = crate::targets::tests_support::two_holder_targets();
    targets.pinned = true;
    targets.size_hint = None;
    assert_eq!(claim_of(&targets).await?, (7, 1));
    Ok(())
}

/// A pinned `--node-id` counts as a holder: its `NotFound` never marks it
/// absent.
#[test]
fn a_pinned_node_is_a_discovered_holder() {
    let (mut targets, _, _) = crate::targets::tests_support::two_holder_targets();
    targets.coverage_by_node.clear();
    let mut nodes = std::collections::HashMap::new();
    assert!(
        holders_from(&targets, &mut nodes)
            .iter()
            .all(|h| !h.discovered_holder)
    );
    targets.pinned = true;
    assert!(
        holders_from(&targets, &mut nodes)
            .iter()
            .all(|h| h.discovered_holder)
    );
}

/// A delegated fetch's slot starts from what earlier fetches signed under its
/// key: every provider's delegate lane in the capability's pool on this
/// deployment, and nothing another key, pool or deployment signed. A swapped-in
/// key counts its own records. The fetch's watermarks go to the delegate lanes.
#[allow(
    clippy::too_many_lines,
    reason = "one fixture: the lane deps, the seeded records, two generations"
)]
#[tokio::test]
async fn a_delegated_slot_counts_its_keys_earlier_spend_from_the_delegate_lanes()
-> anyhow::Result<()> {
    use alloy::primitives::{B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::buyer_pool::BuyerPoolStore;
    use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
    use decdn_incentive::payment_pool::PaymentPool;
    use decdn_incentive::{AdvanceOutcome, Deployment, LaneKey};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::buyer_pool::WatermarkRow;
    use crate::lanes::{LaneDeps, LanePayer};
    use crate::session::{
        ChainAdoption, ChainConfig, DepositPolicy, DiscoveryOptions, PoolOpen, PullSite,
    };

    let credential = |signer: &PrivateKeySigner| -> anyhow::Result<crate::Credential> {
        let capability = decdn_incentive::Capability {
            signer: signer.address(),
            spending_cap: 1_000,
            pool_id: B256::repeat_byte(0x11),
            expiry: u64::MAX,
        }
        .sign(
            &PrivateKeySigner::random(),
            &decdn_incentive::bind_node_id_domain(1, Address::ZERO),
        )?;
        Ok(crate::Credential {
            signer: Arc::new(signer.clone()),
            capability,
        })
    };
    let first = PrivateKeySigner::random();
    let second = PrivateKeySigner::random();
    let slot = {
        let c = credential(&first)?;
        crate::CredentialSlot::new(c.signer, c.capability)?
    };

    let chain: ChainConfig = crate::session::test_support::one_chain(
        "http://config:8545".to_string(),
        1,
        Address::repeat_byte(0x33),
        Address::repeat_byte(0x44),
        None,
    );
    let (pool, passes) = crate::session::test_support::first_pass(&chain)?;
    let deployment = pool.deployment;
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn BuyerPoolStore> =
        Arc::new(RedbBuyerPoolStore::open(&dir.path().join("data"))?);
    let lane = |signer: Address, provider: u8, pool: u8| LaneKey {
        pool_id: B256::repeat_byte(pool),
        signer,
        provider: Address::repeat_byte(provider),
    };
    for (on, key, amount) in [
        (deployment, lane(first.address(), 0xa1, 0x11), 7u64),
        (deployment, lane(first.address(), 0xa2, 0x11), 5),
        (deployment, lane(first.address(), 0xa1, 0x12), 100),
        (
            Deployment {
                chain_id: 2,
                ..deployment
            },
            lane(first.address(), 0xa1, 0x11),
            1_000,
        ),
        (deployment, lane(second.address(), 0xa1, 0x11), 3),
    ] {
        let outcome = store.advance_delegate_progress(
            on,
            key,
            decdn_incentive::BuyerLaneProgress {
                last_amount: U256::from(amount),
                last_bytes: U256::ZERO,
            },
        )?;
        anyhow::ensure!(outcome == AdvanceOutcome::Advanced);
    }

    let ep = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?;
    let rpc = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new());
    let contract = PaymentPool::new(pool.deployment.payment_pool, rpc.clone());
    let site = PullSite {
        data_dir: dir.path().to_path_buf(),
        region: None,
        region_allowlist: Vec::new(),
    };
    let slash_dom = decdn_incentive::slash_judge_domain(1, chain.governance.slash_judge);
    let connections = crate::Connections::new(ep.clone());
    let writes = crate::ordered_writes::OrderedWrites::default();
    let funding = crate::funding::RunFunding::default();
    let deps = LaneDeps {
        marks: None,
        evidence: None,
        endpoint: &ep,
        store: &store,
        contract: &contract,
        rpc: &rpc,
        slash_dom: &slash_dom,
        self_address: first.address(),
        token: Address::repeat_byte(0x22),
        chain: &chain,
        pool: &pool,
        passes: &passes,
        site: &site,
        deposit: DepositPolicy {
            working_deposit: U256::ZERO,
            max_approve: false,
            adoption: ChainAdoption::Refused,
            open: PoolOpen::Allowed,
        },
        namespace_id: decdn_protocol::client::NO_NAMESPACE,
        max_rate_per_mb: 0,
        max_blob_bytes: 0,
        deadlines: crate::PullDeadlines::new(Duration::from_secs(5), Duration::from_secs(5), 0)?,
        connections: &connections,
        writes: &writes,
        funding: &funding,
    };
    let discovery = DiscoveryOptions::default();
    let voucher_dom = deployment.voucher_domain();
    let recovery = Arc::new(crate::RecoveryGate::new());
    let sources = super::LaneSources::new(
        &deps,
        &discovery,
        &[],
        LanePayer::Delegated(&slot),
        &voucher_dom,
        None,
        None,
        None,
        &recovery,
    );

    anyhow::ensure!(sources.watermark_row() == WatermarkRow::Delegated(deployment));
    let _ = sources.credentials();
    anyhow::ensure!(
        slot.recorded_spend(first.address()) == U256::from(12u64),
        "first key recorded {}",
        slot.recorded_spend(first.address())
    );

    let c = credential(&second)?;
    slot.swap(c.signer, c.capability)?;
    let _ = sources.credentials();
    anyhow::ensure!(
        slot.recorded_spend(second.address()) == U256::from(3u64),
        "second key recorded {}",
        slot.recorded_spend(second.address())
    );
    drop(sources);

    // A bundle entry seeds after a sibling entry wrote its lane: the run built
    // the second key's lane at 0xa1 from a prior of 1 and signed it to 3, and
    // the sibling wrote that 3 to the store. The registry counts the 2 past the
    // prior, so the record counts the prior alone, plus the lane at 0xa2 the
    // run holds no ledger for.
    let outcome = store.advance_delegate_progress(
        deployment,
        lane(second.address(), 0xa2, 0x11),
        decdn_incentive::BuyerLaneProgress {
            last_amount: U256::from(4u64),
            last_bytes: U256::ZERO,
        },
    )?;
    anyhow::ensure!(outcome == AdvanceOutcome::Advanced);
    let registry = crate::LaneLedgers::new();
    let built = lane(second.address(), 0xa1, 0x11);
    let _ = registry.get_or_insert(built, || crate::LaneHandle {
        ledger: Arc::new(crate::PoolLedger::new(crate::Cumulative {
            bytes: U256::ZERO,
            amount: U256::from(3u64),
        })),
        ctx: Arc::new(std::sync::Mutex::new(crate::PoolContext {
            pool_id: built.pool_id,
            provider: built.provider,
            deposit: U256::from(1_000u64),
            client_signer: Arc::new(second.clone()),
            voucher_domain: voucher_dom.clone(),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::from(1u64),
            client_binding: None,
            capability: None,
        })),
    });
    let entry = super::LaneSources::new(
        &deps,
        &discovery,
        &[],
        LanePayer::Delegated(&slot),
        &voucher_dom,
        None,
        Some(&registry),
        None,
        &recovery,
    );
    let _ = entry.credentials();
    let recorded = slot.recorded_spend(second.address());
    anyhow::ensure!(
        recorded == U256::from(5u64),
        "a sibling's write counted again: recorded {recorded}"
    );
    anyhow::ensure!(
        recorded + registry.signed_by(second.address()) == U256::from(7u64),
        "the key's spend is every lane's latest amount once"
    );
    drop(entry);

    // A fetch that pays from its own pool writes to the pool row of the payer's
    // owner key: not the lane key its vouchers sign under (#2468), and read
    // from the payer the lanes sign under rather than from the deps.
    let owner = crate::buyer_pool::OwnedPayer::for_store(
        Arc::new(alloy::signers::local::PrivateKeySigner::random()),
        store.as_ref(),
    )?;
    let self_owned = super::LaneSources::new(
        &deps,
        &discovery,
        &[],
        LanePayer::Owned(&owner),
        &voucher_dom,
        None,
        None,
        None,
        &recovery,
    );
    let row = WatermarkRow::Owned {
        deployment: deps.pool.deployment,
        owner: owner.owner().address(),
    };
    anyhow::ensure!(owner.lane().address() != owner.owner().address());
    anyhow::ensure!(self_owned.watermark_row() == row);
    anyhow::ensure!(self_owned.funder().owner == owner.owner().address());
    drop(self_owned);
    ep.close().await;
    Ok(())
}

/// A download, and the empty blob, which a holder serves only whole, take
/// their size from a whole open; a streamed fetch of any other blob opens its
/// first leg (#2644).
#[test]
fn a_size_open_pulls_the_streamed_first_leg_except_for_the_empty_blob() -> anyhow::Result<()> {
    use super::{FirstLeg, size_open_span};
    let streamed = FirstLeg::Streamed {
        window: (0, 0),
        read_ahead: 0,
    };
    anyhow::ensure!(size_open_span([7; 32], FirstLeg::Whole, 0).is_none());
    anyhow::ensure!(
        size_open_span(decdn_protocol::EMPTY_ROOT, streamed, 0).is_none(),
        "a holder refuses any range of the empty blob but the whole"
    );
    let span = size_open_span([7; 32], streamed, 0).ok_or_else(|| anyhow::anyhow!("no span"))?;
    anyhow::ensure!(span.fetch_start() == 0 && span.fetch_len() > 0, "{span:?}");
    Ok(())
}

/// A size lookup streams the blob's first byte, so its open asks for one
/// chunk group however large the blob is.
#[test]
fn a_first_byte_size_open_asks_for_one_chunk_group() -> anyhow::Result<()> {
    use super::{FirstLeg, size_open_span};
    let lookup = FirstLeg::Streamed {
        window: (0, 1),
        read_ahead: 0,
    };
    let span = size_open_span([7; 32], lookup, 0).ok_or_else(|| anyhow::anyhow!("no span"))?;
    anyhow::ensure!(
        span.fetch_start() == 0 && span.fetch_end() == decdn_bao_range::CHUNK_GROUP_BYTES,
        "{span:?}"
    );
    Ok(())
}

/// The primed leg carries the proven size the streamer sizes its store at
/// (#2644).
#[test]
fn the_primed_first_leg_carries_the_streamers_store_size() -> anyhow::Result<()> {
    use super::{FirstLeg, primed_first_leg, size_open_span};
    let window = crate::pacer::PULL_WINDOW_FLOOR;
    let streamed = FirstLeg::Streamed {
        window: (0, 0),
        read_ahead: 0,
    };
    let total = 3 * window + 1234;
    let span = size_open_span([7; 32], streamed, 0).ok_or_else(|| anyhow::anyhow!("no span"))?;
    let leg = primed_first_leg(streamed, 0, &span, total)
        .ok_or_else(|| anyhow::anyhow!("the pull is not the leg"))?;
    anyhow::ensure!(leg.blob_size() == total, "{leg:?}");
    anyhow::ensure!(
        (leg.fetch_start(), leg.fetch_end()) == (span.fetch_start(), span.fetch_end()),
        "{leg:?} vs {span:?}"
    );
    // A blob shorter than the window: the node clamps the pull, which is then
    // the whole blob.
    let short = window / 2;
    let span = size_open_span([7; 32], streamed, 0).ok_or_else(|| anyhow::anyhow!("no span"))?;
    let leg = primed_first_leg(streamed, 0, &span, short)
        .ok_or_else(|| anyhow::anyhow!("the clamped pull is not the leg"))?;
    anyhow::ensure!(leg == decdn_bao_range::align_range(0, 0, short)?, "{leg:?}");
    Ok(())
}

/// Each pass pays only the holders routed to its pool, and a holder that
/// refuses a pool and names another is paid in that pool's pass.
#[expect(
    clippy::too_many_lines,
    reason = "one fixture: the lane deps a real LaneSources needs"
)]
#[tokio::test]
async fn each_pass_pays_the_holders_routed_to_its_pool() -> anyhow::Result<()> {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use decdn_incentive::Deployment;
    use decdn_incentive::buyer_pool::BuyerPoolStore;
    use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
    use decdn_incentive::payment_pool::PaymentPool;

    use crate::SourceProvider as _;
    use crate::lanes::{LaneDeps, LanePayer};
    use crate::pool_route::{PoolPasses, PoolRoute};
    use crate::session::{
        ChainAdoption, ChainConfig, DepositPolicy, DiscoveryOptions, GovernanceChain,
        PaymentPoolChain, PoolOpen, PullSite,
    };

    let pool_on = |chain_id: u64, byte: u8| PaymentPoolChain {
        rpc_url: format!("http://pay-{chain_id}:8545"),
        deployment: Deployment {
            chain_id,
            payment_pool: Address::repeat_byte(byte),
        },
    };
    let (pool_a, pool_b) = (pool_on(42_161, 0xa1), pool_on(8_453, 0xb1));
    let chain = ChainConfig::new(
        GovernanceChain {
            rpc_url: "http://gov:8545".to_string(),
            chain_id: 1,
            slash_judge: Address::repeat_byte(0x44),
            capacity_bond: None,
        },
        vec![pool_a.clone(), pool_b.clone()],
    )?;
    let passes = PoolPasses::new(
        PoolRoute::new(&chain.deployments(), |_| false)
            .ok_or_else(|| anyhow::anyhow!("a route"))?,
    );
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn BuyerPoolStore> =
        Arc::new(RedbBuyerPoolStore::open(&dir.path().join("data"))?);
    let ep = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?;
    let rpc = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new());
    let site = PullSite {
        data_dir: dir.path().to_path_buf(),
        region: None,
        region_allowlist: Vec::new(),
    };
    let slash_dom = decdn_incentive::slash_judge_domain(1, chain.governance.slash_judge);
    let connections = crate::Connections::new(ep.clone());
    let writes = crate::ordered_writes::OrderedWrites::default();
    let funding = crate::funding::RunFunding::default();
    let payer = crate::buyer_pool::OwnedPayer::for_store(
        Arc::new(alloy::signers::local::PrivateKeySigner::random()),
        store.as_ref(),
    )?;
    let discovery = DiscoveryOptions::default();
    let recovery = Arc::new(crate::RecoveryGate::new());

    let node = |seed: u8| crate::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(seed),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let (on_a, on_b, no_hint) = (node(1), node(2), node(3));
    let targets = crate::targets::ResolvedTargets {
        candidates: vec![on_a.clone(), on_b.clone(), no_hint.clone()],
        coverage_by_node: HashMap::new(),
        samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
        opened: crate::targets::OpenedSlot::default(),
        pool_hints: HashMap::from([
            (on_a.node_id, vec![pool_a.deployment.pool_account()]),
            (on_b.node_id, vec![pool_b.deployment.pool_account()]),
        ]),
    };
    assert_eq!(targets.first_pass(&passes), pool_a.deployment);

    let providers_in_pass = |pool: &PaymentPoolChain| -> anyhow::Result<Vec<Address>> {
        let contract = PaymentPool::new(pool.deployment.payment_pool, rpc.clone());
        let deps = LaneDeps {
            marks: None,
            evidence: None,
            endpoint: &ep,
            store: &store,
            contract: &contract,
            rpc: &rpc,
            slash_dom: &slash_dom,
            self_address: payer.owner().address(),
            token: Address::repeat_byte(0x22),
            chain: &chain,
            pool,
            passes: &passes,
            site: &site,
            deposit: DepositPolicy {
                working_deposit: alloy::primitives::U256::ZERO,
                max_approve: false,
                adoption: ChainAdoption::Refused,
                open: PoolOpen::Allowed,
            },
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            max_rate_per_mb: 0,
            max_blob_bytes: 0,
            deadlines: crate::PullDeadlines::new(
                Duration::from_secs(5),
                Duration::from_secs(5),
                0,
            )?,
            connections: &connections,
            writes: &writes,
            funding: &funding,
        };
        let voucher_dom = pool.deployment.voucher_domain();
        let sources = super::LaneSources::new(
            &deps,
            &discovery,
            &[],
            LanePayer::Owned(&payer),
            &voucher_dom,
            None,
            None,
            None,
            &recovery,
        );
        passes.start(pool.deployment);
        let holders = sources.holders_from(&targets);
        // The holder on pool A refuses it and names pool B.
        if pool.deployment == pool_a.deployment
            && let Some(refused) = holders.iter().find(|h| h.provider == on_a.eth_address)
        {
            sources.on_pool_declined(refused, &[pool_b.deployment.pool_account()]);
        }
        Ok(holders.iter().map(|h| h.provider).collect())
    };

    // Pass A: the holder routed to A, and the one with no hint, which takes
    // the first configured pool.
    assert_eq!(
        providers_in_pass(&pool_a)?,
        vec![on_a.eth_address, no_hint.eth_address]
    );
    // Pass B: the holder routed to B, and the retry of the one that refused
    // A.
    assert_eq!(passes.next(), Some(pool_b.deployment));
    assert_eq!(
        providers_in_pass(&pool_b)?,
        vec![on_a.eth_address, on_b.eth_address]
    );
    assert_eq!(passes.next(), None);
    ep.close().await;
    Ok(())
}

/// The parts a delegated [`super::LaneSources`] borrows, with a mocked chain
/// that answers each lane build's `getPool` and `usdc()` reads.
struct LaneFixture {
    _dir: tempfile::TempDir,
    store: std::sync::Arc<dyn decdn_incentive::buyer_pool::BuyerPoolStore>,
    ep: iroh::Endpoint,
    rpc: alloy::providers::RootProvider,
    contract: decdn_incentive::payment_pool::PaymentPool::PaymentPoolInstance<
        alloy::providers::RootProvider,
    >,
    chain: crate::session::ChainConfig,
    pool: crate::session::PaymentPoolChain,
    passes: crate::pool_route::PoolPasses,
    site: crate::session::PullSite,
    slash_dom: alloy::sol_types::Eip712Domain,
    connections: crate::Connections,
    writes: crate::ordered_writes::OrderedWrites,
    funding: crate::funding::RunFunding,
    slot: crate::CredentialSlot,
    voucher_dom: alloy::sol_types::Eip712Domain,
    recovery: std::sync::Arc<crate::RecoveryGate>,
    discovery: crate::session::DiscoveryOptions,
}

impl LaneFixture {
    /// A delegated payer on pool `0x11..`, whose chain answers `lane_builds`
    /// lane builds.
    async fn delegated(lane_builds: usize) -> anyhow::Result<Self> {
        use std::sync::Arc;

        use alloy::primitives::B256;
        use alloy::signers::local::PrivateKeySigner;
        use alloy::sol_types::SolValue as _;
        use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
        use decdn_incentive::payment_pool::PaymentPool;

        let delegate = PrivateKeySigner::random();
        let capability = decdn_incentive::Capability {
            signer: delegate.address(),
            spending_cap: 1_000,
            pool_id: B256::repeat_byte(0x11),
            expiry: u64::MAX,
        }
        .sign(
            &PrivateKeySigner::random(),
            &decdn_incentive::bind_node_id_domain(1, Address::ZERO),
        )?;
        let slot = crate::CredentialSlot::new(Arc::new(delegate), capability)?;
        let chain = crate::session::test_support::one_chain(
            "http://config:8545".to_string(),
            1,
            Address::repeat_byte(0x33),
            Address::repeat_byte(0x44),
            None,
        );
        let (pool, passes) = crate::session::test_support::first_pass(&chain)?;
        let dir = tempfile::tempdir()?;
        let store: Arc<dyn decdn_incentive::buyer_pool::BuyerPoolStore> =
            Arc::new(RedbBuyerPoolStore::open(&dir.path().join("data"))?);
        let ep = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        // The on-chain reads each delegated lane build makes: `getPool`, then
        // the contract's `usdc()`.
        let asserter = alloy::providers::mock::Asserter::new();
        for _ in 0..lane_builds {
            asserter.push_success::<alloy::primitives::Bytes>(
                &PaymentPool::Pool {
                    owner: Address::repeat_byte(0x0C),
                    status: PaymentPool::Status::Open,
                    disputeDeadline: 0,
                    deposit: 1_000_000,
                    totalRedeemed: 0,
                }
                .abi_encode()
                .into(),
            );
            asserter.push_success::<alloy::primitives::Bytes>(
                &Address::repeat_byte(0x22).abi_encode().into(),
            );
        }
        let rpc = alloy::providers::ProviderBuilder::default().connect_mocked_client(asserter);
        let contract = PaymentPool::new(pool.deployment.payment_pool, rpc.clone());
        let site = crate::session::PullSite {
            data_dir: dir.path().to_path_buf(),
            region: None,
            region_allowlist: Vec::new(),
        };
        let slash_dom = decdn_incentive::slash_judge_domain(1, chain.governance.slash_judge);
        let voucher_dom = pool.deployment.voucher_domain();
        Ok(Self {
            _dir: dir,
            store,
            connections: crate::Connections::new(ep.clone()),
            ep,
            rpc,
            contract,
            chain,
            pool,
            passes,
            site,
            slash_dom,
            writes: crate::ordered_writes::OrderedWrites::default(),
            funding: crate::funding::RunFunding::default(),
            slot,
            voucher_dom,
            recovery: Arc::new(crate::RecoveryGate::new()),
            discovery: crate::session::DiscoveryOptions::default(),
        })
    }

    /// The lane deps over the fixture, with `max_rate_per_mb` as the fetch's
    /// rate ceiling.
    fn deps(
        &self,
        max_rate_per_mb: u64,
    ) -> anyhow::Result<crate::lanes::LaneDeps<'_, alloy::providers::RootProvider>> {
        use std::time::Duration;
        Ok(crate::lanes::LaneDeps {
            marks: None,
            evidence: None,
            endpoint: &self.ep,
            store: &self.store,
            contract: &self.contract,
            rpc: &self.rpc,
            slash_dom: &self.slash_dom,
            self_address: Address::repeat_byte(0x0A),
            token: Address::repeat_byte(0x22),
            chain: &self.chain,
            pool: &self.pool,
            passes: &self.passes,
            site: &self.site,
            deposit: crate::session::DepositPolicy {
                working_deposit: alloy::primitives::U256::ZERO,
                max_approve: false,
                adoption: crate::session::ChainAdoption::Refused,
                open: crate::session::PoolOpen::Allowed,
            },
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            max_rate_per_mb,
            max_blob_bytes: 0,
            deadlines: crate::PullDeadlines::new(
                Duration::from_secs(5),
                Duration::from_secs(5),
                0,
            )?,
            connections: &self.connections,
            writes: &self.writes,
            funding: &self.funding,
        })
    }

    /// The fixture's delegated lane sources over `deps`.
    fn sources<'a>(
        &'a self,
        deps: &'a crate::lanes::LaneDeps<'a, alloy::providers::RootProvider>,
    ) -> super::LaneSources<'a, alloy::providers::RootProvider> {
        super::LaneSources::new(
            deps,
            &self.discovery,
            &[],
            crate::lanes::LanePayer::Delegated(&self.slot),
            &self.voucher_dom,
            None,
            None,
            None,
            &self.recovery,
        )
    }
}

/// A blob for the loopback nodes to serve.
fn served_blob() -> Vec<u8> {
    (0..70_000u32).map(|i| (i % 251) as u8).collect()
}

/// A stop policy that gives up after a minute without progress.
fn minute_stop() -> crate::StopPolicy {
    crate::StopPolicy::new(
        false,
        Some(std::time::Duration::from_secs(60)),
        std::sync::Arc::new(crate::ProgressClock::new()),
    )
}

/// Resolve `nodes` for `hash` as the fixture's fetch does: one discovery
/// open per node, with the payer's own context, for `first_leg`'s span.
async fn discover_on<'a>(
    sources: &super::LaneSources<'a, alloy::providers::RootProvider>,
    fx: &'a LaneFixture,
    nodes: &[&crate::opened::fake_node::FakeNode],
    hash: [u8; 32],
    round: crate::targets::OpenRound,
    first_leg: super::FirstLeg,
) -> anyhow::Result<crate::targets::ResolvedTargets> {
    let ctx = sources.discovery_ctx().await?;
    let candidates = nodes
        .iter()
        .map(|n| n.candidate())
        .collect::<anyhow::Result<Vec<_>>>()?;
    crate::targets::open_and_order(
        &candidates,
        None,
        hash,
        crate::targets::OpenSpec {
            conns: &fx.connections,
            ctx: &ctx,
            namespace_id: decdn_protocol::client::NO_NAMESPACE,
            span: super::discovery_span(hash, first_leg, 0),
            max_rate_per_mb: 0,
        },
        warming(false),
        &fx.slash_dom,
        round,
    )
    .await
}

/// Wait until `cond` holds, for at most two seconds.
async fn eventually(what: &str, cond: impl Fn() -> bool) -> anyhow::Result<()> {
    for _ in 0..200 {
        if cond() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    anyhow::bail!("timed out waiting for {what}")
}

/// The first claim adopts the stream discovery opened on the winning holder:
/// the size is the one its answer proved, the node sees one open and no
/// second one, and the lane that answered is parked with the primed pull.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_claim_adopts_the_winners_stream_with_no_second_open() -> anyhow::Result<()> {
    use crate::SourceProvider as _;
    use crate::opened::fake_node::{Answer, FakeNode, Seen};

    let fx = LaneFixture::delegated(1).await?;
    let deps = fx.deps(0)?;
    let sources = fx.sources(&deps);
    let blob = served_blob();
    let hash = *blake3::hash(&blob).as_bytes();
    let node = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 5,
        },
        fx.slash_dom.clone(),
    )
    .await?;
    let targets = discover_on(
        &sources,
        &fx,
        &[&node],
        hash,
        crate::targets::OpenRound::Stream,
        super::FirstLeg::Whole,
    )
    .await?;
    let holders = sources.holders_from(&targets);
    let health = std::sync::Arc::new(crate::PeerHealth::default());
    let claim = sources
        .first_claim(
            hash,
            holders,
            super::FirstLeg::Whole,
            &health,
            &minute_stop(),
        )
        .await?;
    anyhow::ensure!(claim.total_bytes.get() == u64::try_from(blob.len())?);
    anyhow::ensure!(Seen::count(&node.seen.opens) == 1, "one open, adopted");
    anyhow::ensure!(
        Seen::count(&node.seen.resets) == 0,
        "the winner is not reset"
    );
    anyhow::ensure!(
        sources.built(node.operator_address()).is_some(),
        "the lane that adopted the stream is parked for the fetch"
    );
    drop(sources);
    fx.ep.close().await;
    Ok(())
}

/// A streamed fetch's first claim adopts the winner's stream and cancels
/// every other: the loser's node sees its stream reset, and its connection
/// stays open in the command's map. Within one RTT band the cheaper quote
/// wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losers_are_cancelled_with_their_connections_kept() -> anyhow::Result<()> {
    use crate::opened::fake_node::{Answer, FakeNode, Seen};

    let fx = LaneFixture::delegated(1).await?;
    let deps = fx.deps(0)?;
    let sources = fx.sources(&deps);
    let blob = served_blob();
    let hash = *blake3::hash(&blob).as_bytes();
    let leg = super::FirstLeg::Streamed {
        window: (0, 0),
        read_ahead: 16 * 1024,
    };
    let cheap = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 5,
        },
        fx.slash_dom.clone(),
    )
    .await?;
    let dear = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 9,
        },
        fx.slash_dom.clone(),
    )
    .await?;
    let targets = discover_on(
        &sources,
        &fx,
        &[&dear, &cheap],
        hash,
        crate::targets::OpenRound::Settle,
        leg,
    )
    .await?;
    anyhow::ensure!(
        targets.candidates.first().map(|c| c.eth_address) == Some(cheap.operator_address()),
        "the cheaper holder in the same RTT band leads"
    );
    let holders = sources.holders_from(&targets);
    let health = std::sync::Arc::new(crate::PeerHealth::default());
    sources
        .first_claim(hash, holders, leg, &health, &minute_stop())
        .await?;
    anyhow::ensure!(Seen::count(&cheap.seen.opens) == 1, "no second open");
    anyhow::ensure!(
        Seen::count(&cheap.seen.resets) == 0,
        "the winner is adopted"
    );
    eventually("the loser's stream reset", || {
        Seen::count(&dear.seen.resets) == 1
    })
    .await?;
    anyhow::ensure!(dear.connections_open(), "the loser's connection stays open");
    anyhow::ensure!(
        fx.connections.get_or_dial(&dear.addr).await?.is_pinned(),
        "the loser's connection stays pinned in the map"
    );
    drop(sources);
    fx.ep.close().await;
    Ok(())
}

/// A download with two holders adopts the winner's whole-blob stream as its
/// first leg, with no second open, and resets the other: the drive narrows
/// the pull to the winner's stripe and plans the other holder's at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_holder_download_adopts_the_winners_stream() -> anyhow::Result<()> {
    use crate::SourceProvider as _;
    use crate::opened::fake_node::{Answer, FakeNode, Seen};

    let fx = LaneFixture::delegated(1).await?;
    let deps = fx.deps(0)?;
    let sources = fx.sources(&deps);
    let blob = served_blob();
    let hash = *blake3::hash(&blob).as_bytes();
    let serve = |rate| Answer::Serve {
        blob: blob.clone(),
        rate,
    };
    let cheap = FakeNode::spawn(serve(5), fx.slash_dom.clone()).await?;
    let dear = FakeNode::spawn(serve(9), fx.slash_dom.clone()).await?;
    let targets = discover_on(
        &sources,
        &fx,
        &[&dear, &cheap],
        hash,
        crate::targets::OpenRound::Settle,
        super::FirstLeg::Whole,
    )
    .await?;
    let holders = sources.holders_from(&targets);
    let health = std::sync::Arc::new(crate::PeerHealth::default());
    let claim = sources
        .first_claim(
            hash,
            holders,
            super::FirstLeg::Whole,
            &health,
            &minute_stop(),
        )
        .await?;
    anyhow::ensure!(claim.total_bytes.get() == u64::try_from(blob.len())?);
    anyhow::ensure!(claim.holders.len() == 2, "both holders stay for the drive");
    anyhow::ensure!(Seen::count(&cheap.seen.opens) == 1, "no second open");
    anyhow::ensure!(
        Seen::count(&cheap.seen.resets) == 0,
        "the winner is adopted"
    );
    eventually("the loser's stream reset", || {
        Seen::count(&dear.seen.resets) == 1
    })
    .await?;
    anyhow::ensure!(
        sources.built(cheap.operator_address()).is_some(),
        "the winner's lane is parked with the primed pull"
    );
    drop(sources);
    fx.ep.close().await;
    Ok(())
}

/// Record `err` from `faulted` in `set`, then list the providers it starts an
/// hour later, one at a time, with each started one still running.
fn fault_and_next<P: crate::SourceProvider>(
    set: &mut crate::SourceSet<'_, P>,
    faulted: Address,
    err: &anyhow::Error,
) -> Vec<Address> {
    let now = tokio::time::Instant::now();
    set.record_fault(faulted, err, None, now, alloy::primitives::U256::ZERO);
    let later = now + std::time::Duration::from_secs(3600);
    let mut running = std::collections::HashSet::new();
    while let Some(next) = set
        .next_to_start(later, alloy::primitives::U256::ZERO, &running)
        .map(|h| h.provider)
    {
        running.insert(next);
    }
    running.into_iter().collect()
}

/// A node wins the fetch at its discovery quote, then raises its rate on its
/// lane's next leg. The lane refuses that leg unpaid: the node sees no frame
/// on it, and the lane's ledger pays nothing. The node leaves the fetch, so
/// the other holder starts next (ADR 005 § Rate self-enforcement).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leg_quoted_above_the_first_quote_is_refused_unpaid_and_fails_over() -> anyhow::Result<()>
{
    use crate::SourceProvider as _;
    use crate::opened::fake_node::{Answer, FakeNode, Seen};

    let fx = LaneFixture::delegated(1).await?;
    let deps = fx.deps(0)?;
    let sources = fx.sources(&deps);
    let blob = served_blob();
    let hash = *blake3::hash(&blob).as_bytes();
    let raiser = FakeNode::spawn(
        Answer::Reprice {
            blob: blob.clone(),
            first: 5,
            then: 50,
        },
        fx.slash_dom.clone(),
    )
    .await?;
    let honest = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 9,
        },
        fx.slash_dom.clone(),
    )
    .await?;
    let targets = discover_on(
        &sources,
        &fx,
        &[&honest, &raiser],
        hash,
        crate::targets::OpenRound::Settle,
        super::FirstLeg::Whole,
    )
    .await?;
    let holders = sources.holders_from(&targets);
    let health = std::sync::Arc::new(crate::PeerHealth::default());
    let claim = sources
        .first_claim(
            hash,
            holders,
            super::FirstLeg::Whole,
            &health,
            &minute_stop(),
        )
        .await?;
    let lane = sources
        .built(raiser.operator_address())
        .ok_or_else(|| anyhow::anyhow!("the cheaper quote wins, and its lane is parked"))?;
    anyhow::ensure!(lane.source.inner().rate_ceiling().bound() == Some(5));

    // The lane's next leg: the node now quotes 50.
    let err = lane
        .source
        .inner()
        .open_span(hash, 0, 0)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a leg quoted above the first quote is refused"))?;
    let refused = err
        .downcast_ref::<crate::RateAboveCeiling>()
        .ok_or_else(|| anyhow::anyhow!("expected RateAboveCeiling, got {err:#}"))?;
    anyhow::ensure!(refused.quoted_rate_per_mb() == 50 && refused.ceiling_rate_per_mb() == 5);
    anyhow::ensure!(Seen::count(&raiser.seen.opens) == 2, "the leg was answered");
    anyhow::ensure!(
        lane.ledger.committed().amount.is_zero(),
        "the lane paid nothing"
    );

    // The node leaves the fetch: the other holder starts, now and later.
    let mut set = crate::SourceSet::new(&sources, hash, health, claim.holders);
    let next = fault_and_next(&mut set, raiser.operator_address(), &err);
    anyhow::ensure!(next == vec![honest.operator_address()], "{next:?}");
    drop(lane);
    eventually("both of the node's streams end", || {
        Seen::count(&raiser.seen.ended) == 2
    })
    .await?;
    anyhow::ensure!(
        Seen::count(&raiser.seen.paid) == 0,
        "no frame followed either answer"
    );
    drop(set);
    drop(sources);
    fx.ep.close().await;
    Ok(())
}

/// A size lookup takes the size the winner's answer proves with no lane and
/// no open: the chain sees no read, and every stream is reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_size_lookup_takes_the_answers_size_with_no_lane() -> anyhow::Result<()> {
    use crate::SourceProvider as _;
    use crate::opened::fake_node::{Answer, FakeNode, Seen};

    // No lane build: the mocked chain answers no read.
    let fx = LaneFixture::delegated(0).await?;
    let deps = fx.deps(0)?;
    let sources = fx.sources(&deps);
    let blob = served_blob();
    let hash = *blake3::hash(&blob).as_bytes();
    let node = FakeNode::spawn(
        Answer::Serve {
            blob: blob.clone(),
            rate: 5,
        },
        fx.slash_dom.clone(),
    )
    .await?;
    let targets = discover_on(
        &sources,
        &fx,
        &[&node],
        hash,
        crate::targets::OpenRound::Stream,
        super::FirstLeg::Size,
    )
    .await?;
    let holders = sources.holders_from(&targets);
    let health = std::sync::Arc::new(crate::PeerHealth::default());
    let claim = sources
        .first_claim(
            hash,
            holders,
            super::FirstLeg::Size,
            &health,
            &minute_stop(),
        )
        .await?;
    anyhow::ensure!(claim.total_bytes.get() == u64::try_from(blob.len())?);
    anyhow::ensure!(
        sources.built(node.operator_address()).is_none(),
        "a size lookup builds no lane"
    );
    eventually("the stream reset", || Seen::count(&node.seen.resets) == 1).await?;
    anyhow::ensure!(Seen::count(&node.seen.opens) == 1, "no second open");
    drop(sources);
    fx.ep.close().await;
    Ok(())
}

/// A late holder joins the running fetch as a discovered holder with its
/// answered coverage.
#[tokio::test]
async fn a_late_holder_joins_with_its_answered_coverage() -> anyhow::Result<()> {
    use futures_util::StreamExt as _;

    use crate::SourceProvider as _;

    let fx = LaneFixture::delegated(0).await?;
    let deps = fx.deps(0)?;
    let sources = fx.sources(&deps);
    *sources
        .late
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(crate::targets::LateOpens {
        tail: Box::pin(futures_util::stream::iter(vec![late_holder(6, 100.0)])),
        best_holder_rtt_ms: 200.0,
        warming: warming(true),
    });
    let mut arrivals = sources
        .arrivals()
        .ok_or_else(|| anyhow::anyhow!("the late slot holds a round"))?;
    let joined = arrivals
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("the late holder joins"))?;
    anyhow::ensure!(joined.provider == Address::repeat_byte(6));
    anyhow::ensure!(joined.discovered_holder);
    anyhow::ensure!(joined.coverage == Some(decdn_protocol::Coverage::full(4)));
    drop(arrivals);
    drop(sources);
    fx.ep.close().await;
    Ok(())
}

/// A built lane's ceiling is its node's discovery quote under the fetch's
/// `--max-rate-per-mb`: the splice from the deps and the answer through
/// `build_fresh` and `lane_source` into the [`crate::PeerSource`] the lane
/// pays on.
#[tokio::test]
async fn a_built_lane_carries_the_fetch_rate_ceiling() -> anyhow::Result<()> {
    use crate::SourceProvider as _;

    let fx = LaneFixture::delegated(1).await?;
    let deps = fx.deps(25)?;
    let sources = fx.sources(&deps);
    let candidate = crate::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[5; 32]).public(),
        eth_address: Address::repeat_byte(5),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let targets = crate::targets::ResolvedTargets {
        candidates: vec![candidate.clone()],
        coverage_by_node: std::collections::HashMap::new(),
        samples: vec![(candidate.node_id, 10.0, 7)],
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
        opened: crate::targets::OpenedSlot::default(),
        pool_hints: std::collections::HashMap::new(),
    };
    let holders = sources.holders_from(&targets);
    let holder = holders
        .first()
        .ok_or_else(|| anyhow::anyhow!("the candidate is a holder"))?;
    let ended = tokio_util::sync::CancellationToken::new();
    let lane = sources.connect(holder, &ended).await?;
    let ceiling = lane.source.inner().rate_ceiling();
    anyhow::ensure!(
        ceiling.max_rate_per_mb() == 25 && ceiling.bound() == Some(7),
        "the lane's ceiling is the discovery quote under the fetch's cap: {ceiling:?}"
    );
    drop(lane);
    drop(sources);
    fx.ep.close().await;
    Ok(())
}
