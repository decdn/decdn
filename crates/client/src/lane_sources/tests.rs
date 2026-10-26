use super::{PeerEvent, holders_from, lane_lease};
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
        probed_samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
    };
    let mut nodes = std::collections::HashMap::new();
    let holders = holders_from(&targets, &mut nodes);
    assert_eq!(holders.len(), 1, "one holder per operator");
    assert!(
        holders.iter().all(|h| !h.probed_holder),
        "no probe reported the blob"
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
        "a probed holder carries its measured coverage"
    );
    assert!(holders.iter().all(|h| h.probed_holder));
}

fn warming(enabled: bool) -> crate::session::ProxyWarmingParams {
    crate::session::ProxyWarmingParams {
        enabled,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

fn late_non_holder(seed: u8, rtt_ms: f64) -> crate::targets::ProbeOutcome {
    let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    crate::targets::ProbeOutcome::NonHolder(
        crate::discovery::WarmingCandidate {
            node_id,
            eth_address: Address::repeat_byte(seed),
            rtt_ms,
            multiaddrs: alloy::primitives::Bytes::new(),
        },
        (node_id, rtt_ms, 1),
    )
}

fn late_holder(seed: u8, rtt_ms: f64) -> crate::targets::ProbeOutcome {
    let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    crate::targets::ProbeOutcome::Holder(
        crate::discovery::Probed {
            candidate: crate::discovery::NodeCandidate {
                node_id,
                eth_address: Address::repeat_byte(seed),
                region_hint: None,
                multiaddrs: alloy::primitives::Bytes::new(),
            },
            rtt_ms,
            total_bytes: None,
            coverage: decdn_protocol::Coverage::full(4),
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
    assert!(holder.as_ref().is_some_and(|h| h.probed_holder));
    assert!(nodes.contains_key(&Address::repeat_byte(6)));
    Ok(())
}

/// Failed outcomes never join.
#[test]
fn late_join_ignores_failed_outcomes() {
    use super::late_join;
    use crate::targets::ProbeOutcome;
    let mut best = 400.0;
    for outcome in [
        ProbeOutcome::Unreachable,
        ProbeOutcome::RateLimited,
        ProbeOutcome::Unverifiable,
        ProbeOutcome::Unusable,
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
        candidates: vec![n],
        coverage_by_node: std::collections::HashMap::new(),
        probed_samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: crate::targets::LateSlot::default(),
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
                total_bytes: 7,
                holders,
            })
        }
    })
    .await?;
    Ok((
        claim.total_bytes,
        opens.load(std::sync::atomic::Ordering::SeqCst),
    ))
}

/// Holders whose probe gave a size hint take it as the first claim, with
/// no whole-blob open for the header (#2218).
#[tokio::test]
async fn the_first_claim_comes_from_the_probe_hint_without_a_whole_blob_open() -> anyhow::Result<()>
{
    let (targets, _, _) = crate::targets::tests_support::two_holder_targets();
    let hint = targets
        .size_hint
        .ok_or_else(|| anyhow::anyhow!("the fixture's probe gives a hint"))?;
    assert_eq!(claim_of(&targets).await?, (hint, 0));
    Ok(())
}

/// A pinned `--node-id` whose probe gave no size, or a pinned bundle entry with
/// a size from neither its manifest nor a probe, opens one pull for the
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
fn a_pinned_node_is_a_probed_holder() {
    let (mut targets, _, _) = crate::targets::tests_support::two_holder_targets();
    targets.coverage_by_node.clear();
    let mut nodes = std::collections::HashMap::new();
    assert!(
        holders_from(&targets, &mut nodes)
            .iter()
            .all(|h| !h.probed_holder)
    );
    targets.pinned = true;
    assert!(
        holders_from(&targets, &mut nodes)
            .iter()
            .all(|h| h.probed_holder)
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
    use crate::session::{ChainAdoption, ChainConfig, DepositPolicy, DiscoveryOptions, PullSite};

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

    let chain = ChainConfig {
        rpc_url: "http://config:8545".to_string(),
        chain_id: 1,
        payment_pool: Address::repeat_byte(0x33),
        slash_judge: Address::repeat_byte(0x44),
        capacity_bond: None,
    };
    let deployment = chain.deployment();
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
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());
    let site = PullSite {
        data_dir: dir.path().to_path_buf(),
        region: None,
        region_allowlist: Vec::new(),
    };
    let slash_dom = decdn_incentive::slash_judge_domain(1, chain.slash_judge);
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
        site: &site,
        deposit: DepositPolicy {
            working_deposit: U256::ZERO,
            max_approve: false,
            adoption: ChainAdoption::Refused,
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
    let row = WatermarkRow::Owned(owner.owner().address());
    anyhow::ensure!(owner.lane().address() != owner.owner().address());
    anyhow::ensure!(self_owned.watermark_row() == row);
    anyhow::ensure!(self_owned.funder().owner == owner.owner().address());
    drop(self_owned);
    ep.close().await;
    Ok(())
}
