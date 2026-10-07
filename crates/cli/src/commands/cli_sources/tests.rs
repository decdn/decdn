use super::{PeerEvent, holders_from, lane_lease, with_top_up_hint};
use crate::commands::bundle_pull::LaneStreamCap;
use alloy::primitives::{Address, B256, U256};
use decdn_client::NoAffordableSource;

/// Two entries of one command file records for the same node through the
/// command's ordered writes: entry A's first open, then entry B's fault.
/// The fault stays stamped, even with both queued behind a slow write.
#[tokio::test]
async fn an_open_and_a_later_fault_from_two_entries_land_in_order() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    decdn_client::PeerStore::open(dir.path()).upsert_identity(
        &decdn_client::discovery::NodeCandidate {
            node_id,
            eth_address: Address::repeat_byte(3),
            region_hint: None,
            multiaddrs: alloy::primitives::Bytes::new(),
        },
        1_000,
    )?;
    let writes = crate::commands::ordered_writes::OrderedWrites::default();
    let (release, hold) = std::sync::mpsc::channel::<()>();
    writes.queue(move || {
        let _ = hold.recv();
    });
    // Each entry opens its own store handle, as each entry's sources do.
    let entry_a = decdn_client::PeerStore::open(dir.path());
    let entry_b = decdn_client::PeerStore::open(dir.path());
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
    let landed = decdn_client::PeerStore::open(dir.path())
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

/// A self-owned pool no provider's voucher fits names the top-up command
/// with the pool id; any other error passes through.
#[test]
fn a_self_owned_pool_no_voucher_fits_names_the_top_up_command() {
    let pool = B256::repeat_byte(7);
    let err = anyhow::Error::new(NoAffordableSource {
        deposit: U256::from(5u32),
    });
    let hinted = format!("{:#}", with_top_up_hint(err, Some(pool)));
    assert!(
        hinted.contains(&format!(
            "decdn pool top-up --pool {pool} --amount-micro-usdc"
        )),
        "{hinted}"
    );
    let other = with_top_up_hint(anyhow::anyhow!("reset"), Some(pool));
    assert_eq!(other.to_string(), "reset");
}

/// Two nodes of one operator yield one holder: the nearer one by rank.
#[test]
fn one_operator_yields_one_holder_the_nearer_by_rank() {
    let node = |seed: u8| decdn_client::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(0xAA),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let (near, far) = (node(1), node(2));
    let targets = crate::commands::fetch::ResolvedTargets {
        candidates: vec![near.clone(), far],
        coverage_by_node: std::collections::HashMap::new(),
        probed_samples: Vec::new(),
        pinned: false,
        size_hint: None,
        late: crate::commands::fetch::LateSlot::default(),
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
    let (targets, a, b) = crate::commands::fetch::tests_support::two_holder_targets();
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

fn warming(enabled: bool) -> crate::commands::fetch::ProxyWarmingParams {
    crate::commands::fetch::ProxyWarmingParams {
        enabled,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

fn late_non_holder(seed: u8, rtt_ms: f64) -> crate::commands::fetch::ProbeOutcome {
    let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    crate::commands::fetch::ProbeOutcome::NonHolder(
        decdn_client::discovery::WarmingCandidate {
            node_id,
            eth_address: Address::repeat_byte(seed),
            rtt_ms,
            multiaddrs: alloy::primitives::Bytes::new(),
        },
        (node_id, rtt_ms, 1),
    )
}

fn late_holder(seed: u8, rtt_ms: f64) -> crate::commands::fetch::ProbeOutcome {
    let node_id = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    crate::commands::fetch::ProbeOutcome::Holder(
        decdn_client::discovery::Probed {
            candidate: decdn_client::discovery::NodeCandidate {
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
    let near = decdn_client::discovery::NodeCandidate {
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
    use crate::commands::fetch::ProbeOutcome;
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
    let node = |seed: u8| decdn_client::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(0xAA),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    };
    let targets =
        |n: decdn_client::discovery::NodeCandidate| crate::commands::fetch::ResolvedTargets {
            candidates: vec![n],
            coverage_by_node: std::collections::HashMap::new(),
            probed_samples: Vec::new(),
            pinned: false,
            size_hint: None,
            late: crate::commands::fetch::LateSlot::default(),
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
async fn claim_of(
    targets: &crate::commands::fetch::ResolvedTargets,
) -> anyhow::Result<(u64, usize)> {
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
    let (targets, _, _) = crate::commands::fetch::tests_support::two_holder_targets();
    let hint = targets
        .size_hint
        .ok_or_else(|| anyhow::anyhow!("the fixture's probe gives a hint"))?;
    assert_eq!(claim_of(&targets).await?, (hint, 0));
    Ok(())
}

/// A pinned `--node-id` was never probed, so it has no hint and opens one
/// pull for the header's size.
#[tokio::test]
async fn a_pinned_node_without_a_hint_opens_for_the_header() -> anyhow::Result<()> {
    let (mut targets, _, _) = crate::commands::fetch::tests_support::two_holder_targets();
    targets.pinned = true;
    targets.size_hint = None;
    assert_eq!(claim_of(&targets).await?, (7, 1));
    Ok(())
}

/// A pinned `--node-id` counts as a holder: its `NotFound` never marks it
/// absent.
#[test]
fn a_pinned_node_is_a_probed_holder() {
    let (mut targets, _, _) = crate::commands::fetch::tests_support::two_holder_targets();
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
