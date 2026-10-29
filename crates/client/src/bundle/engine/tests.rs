use super::super::{BundleManifest, MalformedManifest, ManifestChunk, NotAManifest};
use super::*;
use alloy::signers::local::PrivateKeySigner;
use iroh::PublicKey;
use std::assert_matches;

/// Every entry draws its own sample of the registry, so a node that one
/// draw leaves out still reaches other entries.
#[test]
fn entry_candidates_resample_the_registry_for_each_entry() {
    let registry = Registry {
        nodes: (1u8..=7).map(|b| stripe_candidate(b, b)).collect(),
        outage_fallback: false,
    };
    let site = site_allowing(&[]);
    let mut seen = HashSet::new();
    let mut draws = HashSet::new();
    for _ in 0..200 {
        let sample = registry.entry_candidates(&site);
        assert_eq!(sample.len(), discovery::SELECT_K);
        let ids: std::collections::BTreeSet<PublicKey> = sample.iter().map(|c| c.node_id).collect();
        seen.extend(ids.iter().copied());
        draws.insert(ids);
    }
    assert_eq!(
        seen.len(),
        registry.nodes.len(),
        "every node reaches some entry"
    );
    assert!(draws.len() > 1, "entries draw different samples");
}

/// A candidate on node `node` in `region`.
fn regional_candidate(node: u8, region: &str) -> NodeCandidate {
    NodeCandidate {
        region_hint: decdn_protocol::Region::parse(region),
        ..stripe_candidate(node, node)
    }
}

/// A site with no region of its own, whose allowlist is `regions`.
fn site_allowing(regions: &[&str]) -> PullSite {
    PullSite {
        data_dir: PathBuf::new(),
        region: None,
        region_allowlist: regions
            .iter()
            .filter_map(|r| decdn_protocol::Region::parse(r))
            .collect(),
    }
}

/// `de` DE nodes then `us` US nodes, as a registry list that is the outage
/// fallback when `outage_fallback`.
fn two_region_registry(de: u8, us: u8, outage_fallback: bool) -> Registry {
    let nodes = (1..=de)
        .map(|b| regional_candidate(b, "DE"))
        .chain((de + 1..=de + us).map(|b| regional_candidate(b, "US")))
        .collect();
    Registry {
        nodes,
        outage_fallback,
    }
}

/// The allowlist narrows every entry's sample to the allowed regions when
/// enough nodes stay: the fetch path's rule, applied to the bundle.
#[test]
fn entry_candidates_keep_to_the_region_allowlist() {
    let registry = two_region_registry(4, 3, false);
    let site = site_allowing(&["US"]);
    let us = decdn_protocol::Region::parse("US");
    for _ in 0..50 {
        let sample = registry.entry_candidates(&site);
        assert_eq!(sample.len(), 3, "the three US nodes, and no DE node");
        assert!(sample.iter().all(|c| c.region_hint == us));
    }
}

/// An allowlist that leaves fewer nodes than the store's freshness floor
/// widens back to every region, so a thin allowlist never starves an entry.
#[test]
fn entry_candidates_widen_when_the_allowlist_starves() {
    let registry = two_region_registry(5, 2, false);
    let site = site_allowing(&["US"]);
    let de = decdn_protocol::Region::parse("DE");
    let sample = registry.entry_candidates(&site);
    assert_eq!(sample.len(), discovery::SELECT_K);
    assert!(
        sample.iter().any(|c| c.region_hint == de),
        "widened past the allowlist"
    );
}

/// The list the peer store supplies during a registry outage is not
/// narrowed: the run is already down to what the store kept.
#[test]
fn a_cached_outage_list_ignores_the_allowlist() {
    let registry = two_region_registry(4, 3, true);
    let site = site_allowing(&["US"]);
    let de = decdn_protocol::Region::parse("DE");
    let sample = registry.entry_candidates(&site);
    assert_eq!(sample.len(), discovery::SELECT_K);
    assert!(
        sample.iter().any(|c| c.region_hint == de),
        "a DE node stays in the sample"
    );
}

/// A peer store holding `n` identities confirmed at `now`.
fn store_with_fresh_identities(dir: &Path, n: u8, now: u64) -> anyhow::Result<()> {
    let store = crate::PeerStore::open(dir);
    for b in 1..=n {
        store.upsert_identity(&stripe_candidate(b, b), now)?;
    }
    Ok(())
}

/// Enough identity-fresh records stand in for the registry; too few, or
/// records past the refresh horizon, leave the run to a live read.
#[test]
fn cached_registry_needs_enough_fresh_identities() -> anyhow::Result<()> {
    let cfg = crate::StoreConfig::default();
    let min = u8::try_from(cfg.min_fresh_candidates)?;
    let now = 1_000_000;

    let enough = tempfile::tempdir()?;
    store_with_fresh_identities(enough.path(), min, now)?;
    let cached = cached_registry(enough.path(), now).map(|c| c.len());
    assert_eq!(cached, Some(cfg.min_fresh_candidates));

    let few = tempfile::tempdir()?;
    store_with_fresh_identities(few.path(), min - 1, now)?;
    assert!(cached_registry(few.path(), now).is_none());

    let later = now + cfg.identity_refresh_secs + 1;
    assert!(cached_registry(enough.path(), later).is_none());
    Ok(())
}

/// A local listener stands in for the RPC endpoint: the chain points at it,
/// the site at `data_dir`, and discovery reads the registry for at most 200
/// ms, forcing the read when `rediscover`.
fn chain_against(
    rpc: &std::net::TcpListener,
    data_dir: &Path,
    rediscover: bool,
) -> anyhow::Result<(ChainConfig, PullSite, DiscoveryOptions)> {
    let chain = ChainConfig {
        rpc_url: format!("http://{}", rpc.local_addr()?),
        chain_id: 1,
        payment_pool: Address::repeat_byte(0x33),
        slash_judge: Address::repeat_byte(0x44),
        capacity_bond: Some(Address::repeat_byte(0x55)),
    };
    let site = PullSite {
        data_dir: data_dir.to_path_buf(),
        region: None,
        region_allowlist: Vec::new(),
    };
    let discovery = DiscoveryOptions {
        pinned: None,
        rediscover,
        max_sources: 4,
        registry_timeout: std::time::Duration::from_millis(200),
        proxy_warming: crate::session::ProxyWarmingParams {
            enabled: true,
            rtt_threshold_ms: 150.0,
            margin_ms: 30.0,
        },
    };
    Ok((chain, site, discovery))
}

/// With fresh identities cached, discovery opens no connection to the RPC
/// endpoint. A forced rediscovery reads the registry.
#[tokio::test]
async fn discover_candidates_skips_the_registry_read_on_fresh_identities() -> anyhow::Result<()> {
    let min = u8::try_from(crate::StoreConfig::default().min_fresh_candidates)?;
    let dir = tempfile::tempdir()?;
    store_with_fresh_identities(dir.path(), min, now_secs())?;
    let rpc = std::net::TcpListener::bind("127.0.0.1:0")?;
    rpc.set_nonblocking(true)?;

    let (chain, site, discovery) = chain_against(&rpc, dir.path(), false)?;
    let found = discover_candidates(&chain, &site, &discovery).await?;
    assert_eq!(found.nodes.len(), usize::from(min));
    assert!(
        !found.outage_fallback,
        "identity-fresh records stand in for a live read"
    );
    assert_matches!(
        rpc.accept(),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock,
        "a cached run must not reach the RPC endpoint"
    );

    let (chain, site, discovery) = chain_against(&rpc, dir.path(), true)?;
    // The listener never answers, so the read times out and falls back to
    // the same store: the outage fallback, which the allowlist leaves alone.
    let found = discover_candidates(&chain, &site, &discovery).await?;
    assert!(rpc.accept().is_ok(), "--rediscover reads the registry");
    assert!(
        found.outage_fallback,
        "a timed-out read falls back to the store"
    );
    assert_eq!(found.nodes.len(), usize::from(min));
    Ok(())
}

/// A blob no provider holds is its entry's fault: it fails its own entry
/// and leaves the rest of the pull running.
#[test]
fn a_blob_no_holder_has_fails_only_its_entry() {
    let err = anyhow::Error::new(crate::NoSourceHasBlob);
    assert_eq!(entry_scope(&err), crate::FatalScope::Item);
    assert!(!ends_the_pull(&err));
}

/// A voucher rejection scopes to its node: on its own it ends no entry's
/// pull beyond its own. "Funding needed" is the one pool's, so every entry
/// would meet it, and it ends the whole pull (ADR 039 §Failure handling).
#[test]
fn only_funding_needed_ends_the_whole_pull() {
    let rejected = anyhow::Error::new(crate::UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    })
    .context("entry");
    assert!(!ends_the_pull(&rejected));
    let funding = anyhow::Error::new(crate::NoAffordableSource {
        deposit: alloy::primitives::U256::ZERO,
    })
    .context("entry");
    assert_eq!(entry_scope(&funding), crate::FatalScope::Command);
    assert!(ends_the_pull(&funding));
}

/// Every node declining one entry fails that entry alone: another
/// entry's nodes can still serve it.
#[test]
fn every_node_declining_fails_only_its_entry() {
    let err = anyhow::Error::new(crate::NoNodeWillServe { reasons: vec![] }).context("entry");
    assert_eq!(entry_scope(&err), crate::FatalScope::Item);
    assert!(!ends_the_pull(&err));
}

/// A blob over the client's cap fails only its own entry.
#[test]
fn an_over_cap_blob_fails_only_its_entry() {
    let err = anyhow::Error::new(crate::BlobTooLarge {
        reached: 2,
        ceiling: 1,
    });
    assert_eq!(entry_scope(&err), crate::FatalScope::Item);
    assert!(!ends_the_pull(&err));
}

/// A give-up is item-scoped, but the clock is the command's, so it ends
/// the pull: every other entry would give up at the same moment.
#[test]
fn a_give_up_ends_the_whole_pull() {
    let err = anyhow::Error::new(crate::GaveUp {
        idle: std::time::Duration::from_secs(1),
    });
    assert_eq!(entry_scope(&err), crate::FatalScope::Item);
    assert!(ends_the_pull(&err));
    assert!(!ends_the_pull(&anyhow!("stream reset")));
}

/// The untracked escrow a handle holds once a `topUp` with tx `byte`s mined
/// and its credit failed.
fn held_escrow(byte: u8) -> UntrackedEscrow {
    let escrows = crate::buyer_pool::UnconfirmedEscrows::default();
    escrows.hold_untracked(&crate::buyer_pool::escrowed_but_untracked(
        "pool 0x01 topped up by 5 µUSDC",
        alloy::primitives::TxHash::repeat_byte(byte),
        "deposit credit lost",
    ));
    escrows.untracked().into_iter().next().unwrap()
}

fn give_up() -> anyhow::Error {
    anyhow::Error::new(crate::GaveUp {
        idle: std::time::Duration::from_secs(1),
    })
}

fn funding_needed() -> anyhow::Error {
    anyhow!("nodes refuse the funding").context(crate::FundingNeeded::PublisherPool {
        pool: alloy::primitives::B256::repeat_byte(0x01),
    })
}

/// The error a pull that is not interrupted ends with, or `None` for one
/// that completed.
fn end_error(end: BundleEnd) -> Option<anyhow::Error> {
    match end {
        BundleEnd::Complete => None,
        BundleEnd::Stopped(error) | BundleEnd::FundingNeeded { error, .. } => Some(error),
        BundleEnd::Interrupted => panic!("the pull ended interrupted"),
    }
}

/// Assert `end` stopped on an untracked escrow that names each of `held`.
fn stopped_on(end: BundleEnd, held: &[&UntrackedEscrow]) -> anyhow::Error {
    let BundleEnd::Stopped(err) = end else {
        panic!("the pull did not end Stopped: {end:?}");
    };
    assert!(
        err.downcast_ref::<crate::buyer_pool::EscrowUntracked>()
            .is_some(),
        "{err:#}"
    );
    for escrow in held {
        assert!(
            format!("{err:#}").contains(&escrow.tx.to_string()),
            "{err:#}"
        );
    }
    err
}

/// A give-up while the puller's handle holds an untracked escrow ends
/// `Stopped` on the escrow, with the give-up kept as text: a caller that
/// reruns a give-up would escrow again (#2511).
#[test]
fn a_give_up_with_an_untracked_escrow_stops_on_the_escrow() {
    let held = held_escrow(0xab);
    let err = stopped_on(
        pull_end(false, Some(give_up()), None, Vec::new(), vec![held.clone()]),
        &[&held],
    );
    assert!(err.downcast_ref::<crate::GaveUp>().is_none(), "{err:#}");
    assert!(format!("{err:#}").contains("no progress"), "{err:#}");
}

/// A pull that completed while its handle holds an untracked escrow stops on
/// it.
#[test]
fn a_complete_pull_with_an_untracked_escrow_stops() {
    let held = held_escrow(0xab);
    stopped_on(
        pull_end(false, None, None, Vec::new(), vec![held.clone()]),
        &[&held],
    );
}

/// A "funding needed" stop while the handle holds an untracked escrow ends
/// `Stopped`, not `FundingNeeded`: a caller that funds the pool and reruns
/// would escrow again.
#[test]
fn funding_needed_with_an_untracked_escrow_stops_on_the_escrow() {
    let held = held_escrow(0xab);
    let err = stopped_on(
        pull_end(
            false,
            Some(funding_needed()),
            None,
            Vec::new(),
            vec![held.clone()],
        ),
        &[&held],
    );
    assert!(
        err.downcast_ref::<crate::FundingNeeded>().is_none(),
        "{err:#}"
    );
}

/// A give-up whose text already names the escrow's tx still stops on the
/// escrow: the text does not stop the caller from rerunning a give-up.
#[test]
fn a_give_up_that_names_the_escrow_still_stops_on_it() {
    let held = held_escrow(0xab);
    let named = give_up().context(format!("a late lane build: {held}"));
    let err = stopped_on(
        pull_end(false, Some(named), None, Vec::new(), vec![held.clone()]),
        &[&held],
    );
    assert!(err.downcast_ref::<crate::GaveUp>().is_none(), "{err:#}");
}

/// A stop that already names the escrow's tx, and does not invite a rerun,
/// is kept as it is.
#[test]
fn a_stop_that_names_the_escrow_is_kept() {
    let held = held_escrow(0xab);
    let named = anyhow!("building the payment lane failed: {held}");
    let err = end_error(pull_end(
        false,
        Some(named),
        None,
        Vec::new(),
        vec![held.clone()],
    ))
    .unwrap();
    assert_eq!(
        format!("{err:#}"),
        format!("building the payment lane failed: {held}")
    );
}

/// Every untracked escrow the handle holds is named, the oldest as the root.
/// A pull that completed is not said to have stopped.
#[test]
fn every_untracked_escrow_is_named() {
    let (first, second) = (held_escrow(0xab), held_escrow(0xcd));
    let both = || vec![first.clone(), second.clone()];
    let err = stopped_on(
        pull_end(false, Some(give_up()), None, Vec::new(), both()),
        &[&first, &second],
    );
    assert!(format!("{err:#}").contains("no progress"), "{err:#}");

    let err = stopped_on(
        pull_end(false, None, None, Vec::new(), both()),
        &[&first, &second],
    );
    let root = err
        .downcast_ref::<crate::buyer_pool::EscrowUntracked>()
        .unwrap();
    assert_eq!(root.tx(), first.tx);
    assert!(!format!("{err:#}").contains("the pull stopped"), "{err:#}");
}

/// A "funding needed" or pool-replaced stop whose text names the escrow's tx
/// still stops on the escrow: a caller that funds the pool, or runs the next
/// pass, would escrow again.
#[test]
fn a_funding_stop_that_names_the_escrow_still_stops_on_it() {
    let held = held_escrow(0xab);
    let named = funding_needed().context(format!("a late lane build: {held}"));
    let err = stopped_on(
        pull_end(false, Some(named), None, Vec::new(), vec![held.clone()]),
        &[&held],
    );
    assert!(
        err.downcast_ref::<crate::FundingNeeded>().is_none(),
        "{err:#}"
    );

    let replaced = PoolReplaced {
        closed: alloy::primitives::B256::repeat_byte(0x02),
        opened: alloy::primitives::B256::repeat_byte(0x03),
    };
    let named = anyhow::Error::new(replaced).context(format!("a late lane build: {held}"));
    let err = stopped_on(
        pull_end(false, Some(named), None, Vec::new(), vec![held.clone()]),
        &[&held],
    );
    assert!(err.downcast_ref::<PoolReplaced>().is_none(), "{err:#}");
}

/// An interrupted pull ends `Interrupted` whatever the handle holds: its
/// caller names the hold.
#[test]
fn an_interrupted_pull_stays_interrupted() {
    let end = pull_end(true, None, None, Vec::new(), vec![held_escrow(0xab)]);
    assert_matches!(end, BundleEnd::Interrupted);
}

/// With no untracked escrow, each end keeps its own type.
#[test]
fn a_pull_with_no_untracked_escrow_ends_as_it_stopped() {
    assert_matches!(
        pull_end(false, None, None, Vec::new(), Vec::new()),
        BundleEnd::Complete
    );
    let err = end_error(pull_end(
        false,
        Some(give_up()),
        None,
        Vec::new(),
        Vec::new(),
    ))
    .unwrap();
    assert!(err.downcast_ref::<crate::GaveUp>().is_some());
    assert_matches!(
        pull_end(false, Some(funding_needed()), None, Vec::new(), Vec::new()),
        BundleEnd::FundingNeeded { .. }
    );
}

/// A second replacement in one run ends the pull "funding needed" and asks
/// for a rerun, unless the handle holds an untracked escrow: then the pull
/// stops on the escrow, and asks for no rerun.
#[test]
fn a_second_replacement_asks_for_a_rerun_only_with_no_untracked_escrow() {
    let replaced = PoolReplaced {
        closed: alloy::primitives::B256::repeat_byte(0x02),
        opened: alloy::primitives::B256::repeat_byte(0x03),
    };
    let err = end_error(pull_end(
        false,
        Some(anyhow::Error::new(replaced)),
        Some(replaced),
        Vec::new(),
        Vec::new(),
    ))
    .unwrap();
    assert!(format!("{err:#}").contains("run the pull again"), "{err:#}");

    let held = held_escrow(0xab);
    let err = stopped_on(
        pull_end(
            false,
            Some(anyhow::Error::new(replaced)),
            Some(replaced),
            Vec::new(),
            vec![held.clone()],
        ),
        &[&held],
    );
    assert!(
        !format!("{err:#}").contains("run the pull again"),
        "{err:#}"
    );
}

/// A command-wide fault stops the pull: no group after it starts, the run
/// halts once, and the fault is what the pull ends with.
#[tokio::test]
async fn a_command_fault_stops_every_later_entry() {
    let started = std::sync::Mutex::new(Vec::new());
    let mut settled = Vec::new();
    let mut halts = 0;
    let stopped = run_groups(
        vec![0u8, 1, 2, 3],
        1,
        |i| {
            started.lock().unwrap().push(i);
            async move {
                if i == 1 {
                    Err(anyhow::Error::new(crate::NoAffordableSource {
                        deposit: alloy::primitives::U256::ZERO,
                    }))
                } else {
                    Ok(())
                }
            }
        },
        |i, r: anyhow::Result<()>| {
            settled.push(i);
            r.err().filter(ends_the_pull)
        },
        || halts += 1,
    )
    .await;
    let err = stopped.expect("the pull stops");
    assert!(err.downcast_ref::<crate::NoAffordableSource>().is_some());
    assert_eq!(*started.lock().unwrap(), vec![0, 1]);
    assert_eq!(settled, vec![0, 1]);
    assert_eq!(halts, 1);
}

/// #2283: with every group in flight and one `--jobs` slot, a group that
/// gives its slot back while it waits for a donor lets the next group
/// start, and finishes once it takes a slot again.
#[tokio::test]
async fn a_donor_waiter_frees_its_slot_for_the_next_group() {
    let gate = JobGate::new(1);
    let order = std::sync::Mutex::new(Vec::new());
    let donor_ready = tokio::sync::Notify::new();
    let run = |i: u8| {
        let (gate, order, donor_ready) = (&gate, &order, &donor_ready);
        async move {
            let slot = FetchSlot::acquire(gate).await?;
            order.lock().unwrap().push(format!("{i}"));
            if i == 0 {
                let ready = donor_ready.notified();
                slot.release();
                ready.await;
                slot.ensure().await?;
                order.lock().unwrap().push("0 again".to_owned());
            }
            if i == 1 {
                donor_ready.notify_one();
            }
            anyhow::Ok(())
        }
    };
    let stopped = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_groups(vec![0u8, 1, 2, 3], 4, run, |_, r| r.err(), || {}),
    )
    .await
    .expect("a waiter that kept its slot would starve group 1");
    assert!(stopped.is_none());
    let order = order.lock().unwrap().clone();
    assert_eq!(order.len(), 5);
    assert_eq!(order.first().map(String::as_str), Some("0"));
    assert_eq!(
        order.get(1).map(String::as_str),
        Some("1"),
        "group 1 starts while group 0 waits"
    );
}

/// #2283: a slot taken again after a donor wait queues behind at most the
/// one new group already in admission, ahead of every group still to
/// start, so an entry whose donors arrived does not wait out the bundle.
#[tokio::test]
async fn a_retaken_slot_waits_behind_at_most_one_new_group() {
    use std::task::Poll;

    use futures_util::poll;

    let gate = JobGate::new(1);
    let waiter = FetchSlot::acquire(&gate).await.expect("first slot");
    let mut b = std::pin::pin!(FetchSlot::acquire(&gate));
    let mut c = std::pin::pin!(FetchSlot::acquire(&gate));
    assert!(poll!(b.as_mut()).is_pending(), "b queues for the permit");
    assert!(poll!(c.as_mut()).is_pending(), "c queues for admission");

    waiter.release();
    let Poll::Ready(Ok(b_slot)) = poll!(b.as_mut()) else {
        panic!("b takes the released permit");
    };
    assert!(
        poll!(c.as_mut()).is_pending(),
        "c is admitted, queued for the permit"
    );
    let mut d = std::pin::pin!(FetchSlot::acquire(&gate));
    assert!(poll!(d.as_mut()).is_pending(), "d queues for admission");
    let mut retake = std::pin::pin!(waiter.ensure());
    assert!(poll!(retake.as_mut()).is_pending());

    drop(b_slot);
    let Poll::Ready(Ok(c_slot)) = poll!(c.as_mut()) else {
        panic!("c, admitted first, takes the next permit");
    };
    assert!(poll!(d.as_mut()).is_pending());
    drop(c_slot);
    assert_matches!(
        poll!(retake.as_mut()),
        Poll::Ready(Ok(())),
        "the retake comes before d"
    );
    assert!(poll!(d.as_mut()).is_pending());
    assert!(waiter.held());
}

/// An item still in flight [`GROUP_DRAIN`] after a command-wide fault is
/// dropped, and its drop guard runs: a fetch dropped this way still persists
/// its lanes' watermarks (`fetch::SettleOnDrop` in `acquire_entry`).
#[tokio::test(start_paused = true)]
async fn a_command_fault_settles_the_items_it_drops() {
    let begun = tokio::time::Instant::now();
    let settled = std::sync::atomic::AtomicU32::new(0);
    let stopped = run_groups(
        vec![0u8, 1],
        2,
        |i| {
            let settled = &settled;
            async move {
                if i == 1 {
                    return Err(anyhow::Error::new(crate::NoAffordableSource {
                        deposit: alloy::primitives::U256::ZERO,
                    }));
                }
                // In flight when its sibling fails: it never finishes.
                let on_drop = SettleOnDrop::new(|| {
                    settled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
                std::future::pending::<()>().await;
                on_drop.disarm();
                Ok(())
            }
        },
        |_, r: anyhow::Result<()>| r.err().filter(ends_the_pull),
        || {},
    )
    .await;
    assert!(stopped.is_some(), "the sibling's fault ends the run");
    assert_eq!(begun.elapsed(), GROUP_DRAIN, "the drain is bounded");
    assert_eq!(
        settled.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the dropped item settled on drop"
    );
}

/// A lane build's escrow that no record credits, as a late build in a
/// sibling's drain reports it: wrapped in [`crate::LaneBuildFault`].
fn late_escrow(byte: u8) -> anyhow::Error {
    anyhow::Error::new(crate::LaneBuildFault(
        crate::buyer_pool::escrowed_but_untracked(
            "pool 0x01 topped up by 5 µUSDC",
            alloy::primitives::TxHash::repeat_byte(byte),
            "deposit credit lost",
        ),
    ))
}

/// Run `ends` (the item that ends the run) beside siblings that each wait
/// for the halt, then end with the error `after_halt` gives them, the way an
/// entry's acquire ends after its drain. Returns the run's error and the
/// items `settle` saw.
async fn halted_siblings(
    ends: anyhow::Error,
    after_halt: impl Fn(u8) -> anyhow::Result<()>,
) -> (Option<anyhow::Error>, Vec<usize>) {
    let halt = CancellationToken::new();
    let ends = std::sync::Mutex::new(Some(ends));
    let mut settled = Vec::new();
    let stopped = run_groups(
        vec![0u8, 1, 2],
        3,
        |i| {
            let (halt, ends, after_halt) = (&halt, &ends, &after_halt);
            async move {
                if i == 0 {
                    return Err(ends.lock().unwrap().take().unwrap());
                }
                halt.cancelled().await;
                after_halt(i)
            }
        },
        |i, r: anyhow::Result<()>| {
            settled.push(i);
            r.err().filter(ends_the_pull)
        },
        || halt.cancel(),
    )
    .await;
    settled.sort_unstable();
    (stopped, settled)
}

/// #2509: once one entry ends the pull, the siblings in flight are halted,
/// not dropped: each runs to its end and is settled, and an error that holds
/// no escrow is dropped, so the first fault is what the pull ends with.
#[tokio::test]
async fn a_command_fault_halts_the_siblings_and_waits_for_them() {
    let (stopped, settled) =
        halted_siblings(give_up(), |_| Err(anyhow::Error::new(crate::stop::Halted))).await;
    let err = stopped.expect("the pull stops");
    assert!(err.downcast_ref::<crate::GaveUp>().is_some(), "{err:#}");
    assert_eq!(settled, vec![0, 1, 2]);
}

/// The error a halted sibling's acquire returns when a lane build in its
/// drain escrowed: the escrow, with the halt folded in as text.
fn halted_with_late_escrow(byte: u8) -> anyhow::Error {
    let halted = anyhow::Error::new(crate::stop::Halted);
    fold_late_escrow(Err::<(), _>(halted), Some(late_escrow(byte)))
        .err()
        .unwrap()
}

/// #2509: an entry gives up while a sibling's late lane build escrows USDC no
/// record credits. The pull ends on the escrow, with the give-up kept as
/// text, so the caller does not exit 75 and rerun into a second escrow.
#[tokio::test]
async fn a_siblings_late_escrow_wins_over_a_give_up() {
    let (stopped, _) = halted_siblings(give_up(), |i| {
        if i == 1 {
            Err(halted_with_late_escrow(0xab))
        } else {
            Err(anyhow::Error::new(crate::stop::Halted))
        }
    })
    .await;
    let err = stopped.expect("the pull stops");
    assert!(err.downcast_ref::<crate::GaveUp>().is_none(), "{err:#}");
    assert!(carries_untracked_escrow(&err), "{err:#}");
    let tx = alloy::primitives::TxHash::repeat_byte(0xab).to_string();
    let text = format!("{err:#}");
    assert!(text.contains("no progress"), "{text}");
    assert_eq!(text.matches(&tx).count(), 1, "{text}");
    // The handle holds the same escrow: the pull's end names it once, and
    // does not invite a rerun.
    let end = pull_end(false, Some(err), None, Vec::new(), vec![held_escrow(0xab)]);
    let BundleEnd::Stopped(err) = end else {
        panic!("the pull did not end Stopped: {end:?}");
    };
    assert!(!invites_rerun(&err), "{err:#}");
    assert_eq!(format!("{err:#}").matches(&tx).count(), 1, "{err:#}");
}

/// #2509: two siblings that each escrowed untracked both reach the error.
#[tokio::test]
async fn every_siblings_late_escrow_is_named() {
    let (stopped, _) = halted_siblings(give_up(), |i| Err(late_escrow(i))).await;
    let text = format!("{:#}", stopped.expect("the pull stops"));
    for byte in [1u8, 2] {
        let tx = alloy::primitives::TxHash::repeat_byte(byte).to_string();
        assert!(text.contains(&tx), "{text}");
    }
}

/// #2509: a sibling's own command-wide fault in the drain is not an escrow:
/// the first fault stays what the pull ends with.
#[tokio::test]
async fn a_siblings_other_fault_does_not_replace_the_first() {
    let (stopped, _) = halted_siblings(give_up(), |_| {
        Err(anyhow::Error::new(crate::NoAffordableSource {
            deposit: alloy::primitives::U256::ZERO,
        }))
    })
    .await;
    let err = stopped.expect("the pull stops");
    assert!(err.downcast_ref::<crate::GaveUp>().is_some(), "{err:#}");
    assert!(
        err.downcast_ref::<crate::NoAffordableSource>().is_none(),
        "{err:#}"
    );
}

/// #2509: a halted fetch leaves each destination it would write `NotRun`,
/// keeps the slots it had already classified, and ends nothing. The summary
/// counts the `NotRun` ones as not run (#2517).
#[test]
fn a_halted_fetch_marks_its_writes_not_run() {
    let run = GroupRun::fetch_failed(
        vec![
            Slot::Skip,
            Slot::Write {
                label: "a.bin",
                dest: PathBuf::from("a.bin"),
            },
            Slot::Failed(EntryOutcome::failed("b.bin", &anyhow!("bad path"))),
        ],
        anyhow::Error::new(crate::stop::Halted).context("entry a.bin"),
    );
    assert!(run.stop.is_none());
    assert_matches!(
        run.outcomes.as_slice(),
        [
            EntryOutcome::Skipped,
            EntryOutcome::NotRun,
            EntryOutcome::Failed { .. }
        ]
    );
    let report = bundle_report(
        &run.outcomes,
        Transfer::default(),
        DedupSummary::default(),
        Vec::new(),
    );
    assert_eq!(
        (
            report.fetched,
            report.skipped,
            report.failed,
            report.not_run
        ),
        (0, 1, 1, 1),
        "a NotRun entry counts as not run"
    );
}

/// #2509: a halted fetch whose drain escrowed is not halted: it fails its
/// entries and ends the pull on the escrow.
#[test]
fn a_halted_fetch_whose_build_escrowed_ends_the_pull() {
    let run = GroupRun::fetch_failed(
        vec![Slot::Write {
            label: "a.bin",
            dest: PathBuf::from("a.bin"),
        }],
        halted_with_late_escrow(0xab),
    );
    assert_matches!(run.outcomes.as_slice(), [EntryOutcome::Failed { .. }]);
    let stop = run.stop.expect("the escrow ends the pull");
    assert!(carries_untracked_escrow(&stop), "{stop:#}");
}

/// #2509: an entry keeps its first-pass outcome, landed or failed, when the
/// second pass did not run it.
#[test]
fn a_landed_entry_survives_a_second_pass_that_did_not_run_it() {
    let pass = |outcomes| PullRun {
        outcomes,
        warnings: Vec::new(),
        transfer: Transfer::default(),
        interrupted: false,
        stopped: None,
    };
    let merged = merge_passes(
        pass(vec![
            EntryOutcome::Fetched(5),
            EntryOutcome::NotRun,
            EntryOutcome::failed("c.bin", &anyhow!("disk full")),
        ]),
        pass(vec![
            EntryOutcome::NotRun,
            EntryOutcome::Fetched(7),
            EntryOutcome::NotRun,
        ]),
    );
    assert_matches!(
        merged.outcomes.as_slice(),
        [
            EntryOutcome::Fetched(5),
            EntryOutcome::Fetched(7),
            EntryOutcome::Failed { .. }
        ]
    );
}

/// A first pass's run that ended on `stopped`.
fn ended_on(stopped: Option<anyhow::Error>, interrupted: bool) -> PullRun {
    PullRun {
        outcomes: Vec::new(),
        warnings: Vec::new(),
        transfer: Transfer::default(),
        interrupted,
        stopped,
    }
}

/// The replacement of pool `closed` by pool `opened`.
fn replacement(closed: u8, opened: u8) -> PoolReplaced {
    PoolReplaced {
        closed: alloy::primitives::B256::repeat_byte(closed),
        opened: alloy::primitives::B256::repeat_byte(opened),
    }
}

/// #2517: a replacement that ends the first pass runs the rest again.
#[test]
fn a_replacement_that_ends_the_pass_runs_again() {
    let new = replacement(0x01, 0x02);
    let run = ended_on(Some(anyhow::Error::new(new)), false);
    assert_eq!(replaced_pool(&run), Some(new));
}

/// #2517: a sibling entry's recovery step replaces the pool while another
/// entry's give-up ends the pass. Nothing runs again, since the pull ends;
/// the gate lists the replacement, so the caller names the closed pool's
/// balance to reclaim.
#[test]
fn a_give_up_beside_a_siblings_replacement_runs_nothing_again() {
    let run = ended_on(Some(give_up()), false);
    assert_eq!(replaced_pool(&run), None);
}

/// #2517: a late escrow folded over the `PoolReplaced` that ended the pass
/// leaves the replacement only as text, so it runs nothing again.
#[test]
fn a_replacement_under_a_late_escrow_runs_nothing_again() {
    let new = replacement(0x01, 0x02);
    let stopped = crate::scheduler::fold_late_escrow(
        Err::<(), _>(anyhow::Error::new(new)),
        Some(late_escrow(0xab)),
    )
    .err();
    assert_eq!(replaced_pool(&ended_on(stopped, false)), None);
}

/// #2517: an interrupted pass runs nothing again, and a pass with no
/// replacement has none to run against.
#[test]
fn an_interrupted_replacement_runs_nothing_again() {
    let new = replacement(0x02, 0x03);
    let run = ended_on(Some(anyhow::Error::new(new)), true);
    assert_eq!(replaced_pool(&run), None);
    assert_eq!(replaced_pool(&ended_on(None, false)), None);
}

/// #2519: a manifest fetch that a replacement ended runs nothing again, so
/// its error names the new pool as the current one to run the pull against.
/// Any other fault keeps its own text.
#[test]
fn a_replaced_manifest_fetch_names_the_new_pool() {
    let new = replacement(0x01, 0x02);
    let err = manifest_fetch_error(anyhow::Error::new(new));
    let text = format!("{err:#}");
    assert!(
        text.contains(&format!("pool {} is now the current pool", new.opened)),
        "{text}"
    );
    assert!(text.starts_with("fetch bundle manifest blob: "), "{text}");
    assert_eq!(err.downcast_ref::<PoolReplaced>(), Some(&new));
    assert_eq!(
        format!("{:#}", manifest_fetch_error(anyhow::anyhow!("not found"))),
        "fetch bundle manifest blob: not found"
    );
}

/// A gate that holds the replacement of pool `0x01` by pool `0x02`, as a
/// recovery step leaves it.
async fn gate_replaced() -> RecoveryGate {
    let gate = RecoveryGate::new();
    let funder = crate::source::FakeFunder::new(crate::Recovery::Replaced(replacement(0x01, 0x02)));
    let mut top_ups = 0;
    let deposit = alloy::primitives::U256::from(1_000u64);
    let _ = gate
        .step(
            &funder,
            deposit,
            &mut top_ups,
            || deposit,
            alloy::primitives::U256::ZERO,
        )
        .await;
    gate
}

/// A pull that starts with a replacement on the gate from an earlier pull
/// (here reported without a rerun) starts as a pass against the new pool. Its
/// funding facts name no pool yet, so its recovery step funds the pool its
/// lanes reuse, not the closed one, which would open yet another pool. The
/// escrow handle carries over.
#[tokio::test]
async fn a_pull_after_a_carried_replacement_starts_on_the_new_pool() {
    let gate = gate_replaced().await;
    let escrows = crate::buyer_pool::UnconfirmedEscrows::default();
    let mut funding = RunFunding::reporting_escrows_to(escrows.clone());
    funding.note_pool(alloy::primitives::B256::repeat_byte(0x01));
    let mut ledgers = LaneLedgers::new();
    leave_carried_replacement(&gate, &mut ledgers, &mut funding);
    assert_eq!(gate.replaced(), None, "the gate steps on the new pool");
    assert_eq!(
        gate.replacements(),
        [replacement(0x01, 0x02)],
        "the gate still lists it, for the caller to name"
    );
    assert_eq!(funding.pool_id(), None, "the closed pool is not the run's");
    let _ = escrows.hold_untracked(&crate::buyer_pool::escrowed_but_untracked(
        "pool 0x01 topped up by 5 µUSDC",
        alloy::primitives::TxHash::repeat_byte(0xab),
        "disk full",
    ));
    assert_eq!(funding.escrows().untracked().len(), 1, "the same handle");

    // With nothing carried, the run keeps its pool.
    let gate = RecoveryGate::new();
    let mut funding = RunFunding::default();
    funding.note_pool(alloy::primitives::B256::repeat_byte(0x02));
    leave_carried_replacement(&gate, &mut ledgers, &mut funding);
    assert_eq!(
        funding.pool_id(),
        Some(alloy::primitives::B256::repeat_byte(0x02))
    );
}

/// #2517: a group the pull never settled counts each of its entries as not
/// run, so the counts add up to the entry count.
#[test]
fn a_group_that_never_settled_is_not_run() {
    let groups = [
        SettledGroup {
            outcomes: vec![EntryOutcome::Fetched(5)],
            ..SettledGroup::default()
        },
        SettledGroup::not_run(2),
    ];
    let outcomes: Vec<_> = groups.into_iter().flat_map(|g| g.outcomes).collect();
    assert_matches!(
        outcomes.as_slice(),
        [
            EntryOutcome::Fetched(5),
            EntryOutcome::NotRun,
            EntryOutcome::NotRun
        ]
    );
    let report = bundle_report(
        &outcomes,
        Transfer::default(),
        DedupSummary::default(),
        Vec::new(),
    );
    assert_eq!((report.fetched, report.not_run), (1, 2));
}

/// A `topUp` with tx `byte`s whose receipt was not read.
fn pending_top_up(byte: u8) -> EscrowUnconfirmed {
    EscrowUnconfirmed::TopUp(crate::buyer_pool::TopUpUnconfirmed {
        tx: Some(alloy::primitives::TxHash::repeat_byte(byte)),
        nonce: 7,
    })
}

/// #2517: an entry dropped while its `topUp` receipt read was pending leaves
/// the tx on the handle. A pull that is not interrupted ends `Stopped` on it,
/// whatever it ended with, so a library caller hears of it and a give-up
/// cannot invite a rerun.
#[test]
fn a_pending_top_up_stops_the_pull_on_it() {
    let pending = pending_top_up(0xcd);
    for stopped in [None, Some(give_up())] {
        let BundleEnd::Stopped(err) = pull_end(false, stopped, None, vec![pending], Vec::new())
        else {
            panic!("the pull did not end Stopped");
        };
        assert!(
            err.downcast_ref::<crate::buyer_pool::TopUpUnconfirmed>()
                .is_some(),
            "{err:#}"
        );
        assert!(err.downcast_ref::<crate::GaveUp>().is_none(), "{err:#}");
        assert!(format!("{err:#}").contains(&pending.to_string()), "{err:#}");
    }
    assert_matches!(
        pull_end(true, None, None, vec![pending], Vec::new()),
        BundleEnd::Interrupted
    );
}

/// #2517: an untracked escrow stays the root over a pending one, and both are
/// named.
#[test]
fn an_untracked_escrow_is_the_root_over_a_pending_one() {
    let held = held_escrow(0xab);
    let pending = pending_top_up(0xcd);
    let err = stopped_on(
        pull_end(false, None, None, vec![pending], vec![held.clone()]),
        &[&held],
    );
    assert!(format!("{err:#}").contains(&pending.to_string()), "{err:#}");
}

/// #2517: a stop that already names the pending escrow keeps it as the
/// error, and names it once.
#[test]
fn a_stop_that_names_the_pending_escrow_keeps_it() {
    let pending = pending_top_up(0xcd);
    let named = pending.into_error().context("entry a.bin");
    let err = end_error(pull_end(
        false,
        Some(named),
        None,
        vec![pending],
        Vec::new(),
    ))
    .expect("the pull stops");
    let text = format!("{err:#}");
    assert_eq!(text.matches(&pending.to_string()).count(), 1, "{text}");
}

/// #2509: a pass that ran on a halt it did not set fails, rather than end
/// complete with its halted entries never run.
#[test]
fn a_pass_on_a_stale_halt_does_not_end_complete() {
    assert!(pass_stop(None, false).is_none());
    let stale = pass_stop(None, true).expect("a stale halt fails the pass");
    assert_matches!(
        pull_end(false, Some(stale), None, Vec::new(), Vec::new()),
        BundleEnd::Stopped(_)
    );
    let fault = pass_stop(Some(give_up()), true).expect("the pass's own fault");
    assert!(fault.downcast_ref::<crate::GaveUp>().is_some(), "{fault:#}");
}

/// #2509: a sibling's escrow that lands before the drain's bound is kept when
/// another sibling outlives the bound and drops.
#[tokio::test(start_paused = true)]
async fn an_escrow_before_the_drain_bound_survives_a_dropped_sibling() {
    let halt = CancellationToken::new();
    let begun = tokio::time::Instant::now();
    let stopped = run_groups(
        vec![0u8, 1, 2],
        3,
        |i| {
            let halt = &halt;
            async move {
                match i {
                    0 => Err(give_up()),
                    1 => {
                        halt.cancelled().await;
                        Err(halted_with_late_escrow(0xab))
                    }
                    _ => {
                        halt.cancelled().await;
                        std::future::pending().await
                    }
                }
            }
        },
        |_, r: anyhow::Result<()>| r.err().filter(ends_the_pull),
        || halt.cancel(),
    )
    .await;
    let err = stopped.expect("the pull stops");
    assert!(carries_untracked_escrow(&err), "{err:#}");
    assert!(format!("{err:#}").contains("no progress"), "{err:#}");
    assert_eq!(begun.elapsed(), GROUP_DRAIN);
}

/// An item-scoped fault fails its own entry and the pull runs on.
#[tokio::test]
async fn an_item_fault_fails_only_its_entry() {
    let mut failed = Vec::new();
    let stopped = run_groups(
        vec![0u8, 1, 2],
        2,
        |i| async move {
            if i == 1 {
                Err(anyhow::Error::new(crate::BlobTooLarge {
                    reached: 2,
                    ceiling: 1,
                }))
            } else {
                Ok(())
            }
        },
        |i, r: anyhow::Result<()>| match r {
            Ok(()) => None,
            Err(e) => {
                failed.push(i);
                Some(e).filter(ends_the_pull)
            }
        },
        || panic!("an item fault does not halt the run"),
    )
    .await;
    assert!(stopped.is_none());
    assert_eq!(failed, vec![1]);
}

/// One clock for the command: a stuck entry keeps waiting while a sibling
/// lands bytes, and gives up only once nothing in the pull has progressed
/// for the whole limit.
#[tokio::test(start_paused = true)]
async fn a_stuck_entry_waits_while_another_progresses() {
    let limit = std::time::Duration::from_secs(10);
    let stop = crate::StopPolicy::new(false, Some(limit), Arc::new(crate::ProgressClock::new()));
    let begun = tokio::time::Instant::now();
    let stopped = run_groups(
        vec![0u8, 1],
        2,
        |i| {
            let stop = stop.clone();
            async move {
                if i == 0 {
                    // A stuck entry: no byte ever lands.
                    return Err(anyhow::Error::new(stop.expired().await));
                }
                // A sibling that lands a byte every 5 s for 30 s.
                for _ in 0..6 {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    stop.clock.tick();
                }
                Ok(())
            }
        },
        |_, r: anyhow::Result<()>| r.err().filter(ends_the_pull),
        || stop.halt(),
    )
    .await;
    assert!(
        stopped
            .as_ref()
            .is_some_and(|e| e.downcast_ref::<crate::GaveUp>().is_some())
    );
    assert_eq!(begun.elapsed(), std::time::Duration::from_secs(40));
}

#[test]
fn build_completed_updates_records_present_omits_failed() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("ok.txt"), b"data").expect("write");
    // "bad.txt" has an OLD file present on disk (this run's fetch failed and
    // left it untouched) — it must never be recorded with the NEW hash.
    std::fs::write(tmp.path().join("bad.txt"), b"old-bytes").expect("write");
    let entries = [
        ManifestEntry {
            path: "ok.txt".into(),
            hash: [0xaa; 32],
            size: Some(4),
            chunks: Some(vec![ManifestChunk {
                hash: [0xbb; 32],
                size: 4,
            }]),
            landed_from: None,
        },
        ManifestEntry {
            path: "bad.txt".into(),
            hash: [0xcc; 32],
            size: Some(9),
            chunks: None,
            landed_from: None,
        },
    ];
    let outcomes = vec![
        EntryOutcome::Fetched(4),
        EntryOutcome::Failed {
            path: "bad.txt".into(),
            err: "nope".into(),
        },
    ];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let upd = build_completed_updates(&refs, &outcomes, TEST_BUNDLE, tmp.path());
    assert!(upd.contains_key("ok.txt"));
    let rec = upd.get("ok.txt").expect("rec");
    assert_eq!(rec.hash, format_hash([0xaa; 32]));
    assert_eq!(rec.size, 4);
    assert!(rec.chunks.is_some());
    // Failed → omitted even though the (stale) file is present on disk.
    assert!(!upd.contains_key("bad.txt"));
}

#[test]
fn build_completed_updates_records_linked_and_skipped() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("linked.txt"), b"aa").expect("write");
    std::fs::write(tmp.path().join("skipped.txt"), b"bbbb").expect("write");
    let entries = [
        ManifestEntry {
            path: "linked.txt".into(),
            hash: [0x11; 32],
            size: Some(2),
            chunks: None,
            landed_from: None,
        },
        ManifestEntry {
            path: "skipped.txt".into(),
            hash: [0x22; 32],
            size: Some(4),
            chunks: None,
            landed_from: None,
        },
    ];
    let outcomes = vec![EntryOutcome::Linked, EntryOutcome::Skipped];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let upd = build_completed_updates(&refs, &outcomes, TEST_BUNDLE, tmp.path());
    // Both are successes this run and present on disk → both recorded.
    assert_eq!(
        upd.get("linked.txt").expect("linked").hash,
        format_hash([0x11; 32])
    );
    assert_eq!(
        upd.get("skipped.txt").expect("skipped").hash,
        format_hash([0x22; 32])
    );
}

#[test]
fn build_completed_updates_omits_entry_absent_from_outcomes() {
    // The mid-run trap: an entry whose group has NOT completed yet is simply
    // not in `outcomes`. Even with a stale file already on disk under the new
    // hash, `zip` never reaches it, so it is never recorded from bytes this
    // run has not landed. (Here only the first entry has an outcome.)
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("done.txt"), b"new").expect("write");
    std::fs::write(tmp.path().join("pending.txt"), b"stale-old-bytes").expect("write");
    let entries = [
        ManifestEntry {
            path: "done.txt".into(),
            hash: [0x33; 32],
            size: Some(3),
            chunks: None,
            landed_from: None,
        },
        ManifestEntry {
            path: "pending.txt".into(),
            hash: [0x44; 32], // new hash, not yet fetched
            size: Some(3),
            chunks: None,
            landed_from: None,
        },
    ];
    let outcomes = vec![EntryOutcome::Fetched(3)]; // only "done.txt" has completed
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let upd = build_completed_updates(&refs, &outcomes, TEST_BUNDLE, tmp.path());
    assert!(upd.contains_key("done.txt"));
    assert!(
        !upd.contains_key("pending.txt"),
        "an entry not yet in outcomes must never be recorded"
    );
}

/// The bundle key tests record their skip-cache files under.
const TEST_BUNDLE: [u8; 32] = [0; 32];

/// A skip-cache source in [`TEST_BUNDLE`], for records whose source no check reads.
fn test_source() -> skip_cache::SavedSource {
    skip_cache::SavedSource::new(TEST_BUNDLE, "test")
}

fn saved_file(hash: &str, size: u64) -> skip_cache::SavedFile {
    skip_cache::SavedFile {
        hash: hash.into(),
        size,
        mtime: SavedMtime { secs: 1, nanos: 0 },
        chunks: None,
        source: test_source(),
    }
}

fn one_update(path: &str, hash: &str, size: u64) -> BTreeMap<String, skip_cache::SavedFile> {
    let mut m = BTreeMap::new();
    m.insert(path.to_string(), saved_file(hash, size));
    m
}

// A cancel mid-pull drops the drive while an entry is still in flight. The
// flush must still write every entry that settled before it.
#[tokio::test]
async fn an_interrupted_drive_still_flushes_the_settled_entries() {
    let tmp = tempfile::tempdir().expect("tmp");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let fire = cancel.clone();
    let drive = async move {
        tx.send(FlushBatch {
            updates: one_update("done.bin", "b3:done", 1),
            fetched_bytes: 1,
        })
        .expect("send");
        // Cancel while another entry is still in flight: it never settles.
        fire.cancel();
        std::future::pending::<()>().await;
    };
    let flush = flush_task(tmp.path(), SavedManifest::default(), rx);

    assert!(drive_until_interrupted(drive, flush, &cancel).await);
    let saved = skip_cache::load(tmp.path());
    assert_eq!(saved.get("done.bin").expect("done").hash, "b3:done");
}

#[tokio::test]
async fn a_drive_that_finishes_is_not_interrupted() {
    let tmp = tempfile::tempdir().expect("tmp");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<FlushBatch>();
    let drive = async move { drop(tx) };
    let cancel = CancellationToken::new();
    let flush = flush_task(tmp.path(), SavedManifest::default(), rx);
    assert!(!drive_until_interrupted(drive, flush, &cancel).await);
}

// Several sub-cadence batches, then the channel closes: the final write must
// persist every batch — an interrupted pull keeps all completed files.
#[tokio::test]
async fn flush_task_final_write_persists_all_batches() {
    let tmp = tempfile::tempdir().expect("tmp");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(FlushBatch {
        updates: one_update("a.bin", "b3:a", 1),
        fetched_bytes: 1,
    })
    .expect("send a");
    tx.send(FlushBatch {
        updates: one_update("b.bin", "b3:b", 1),
        fetched_bytes: 1,
    })
    .expect("send b");
    drop(tx);
    flush_task(tmp.path(), SavedManifest::default(), rx).await;

    let saved = skip_cache::load(tmp.path());
    assert_eq!(saved.get("a.bin").expect("a").hash, "b3:a");
    assert_eq!(saved.get("b.bin").expect("b").hash, "b3:b");
}

// The flush folds onto the prior skip-cache: entries a prior run recorded but
// this run never touched survive, alongside this run's new records.
#[tokio::test]
async fn flush_task_keeps_prior_untouched_entries() {
    let tmp = tempfile::tempdir().expect("tmp");
    let mut prior = SavedManifest::default();
    skip_cache::merge(&mut prior, one_update("old.bin", "b3:old", 9));

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(FlushBatch {
        updates: one_update("new.bin", "b3:new", 2),
        fetched_bytes: 2,
    })
    .expect("send");
    drop(tx);
    flush_task(tmp.path(), prior, rx).await;

    let saved = skip_cache::load(tmp.path());
    assert_eq!(saved.get("old.bin").expect("old kept").hash, "b3:old");
    assert_eq!(saved.get("new.bin").expect("new recorded").hash, "b3:new");
}

// A byte-cadence flush mid-stream must persist before the channel closes: a
// single batch over FLUSH_BYTES is written while the task still runs. Proven
// by observing the file after that batch, before dropping the sender.
#[tokio::test]
async fn flush_task_byte_cadence_writes_mid_stream() {
    let tmp = tempfile::tempdir().expect("tmp");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn({
        let root = tmp.path().to_path_buf();
        async move { flush_task(&root, SavedManifest::default(), rx).await }
    });
    tx.send(FlushBatch {
        updates: one_update("big.bin", "b3:big", FLUSH_BYTES),
        fetched_bytes: FLUSH_BYTES,
    })
    .expect("send");
    // Poll for the mid-stream write while the task is still alive (sender held).
    // A generous deadline absorbs fsync + scheduling latency on slow CI, while
    // still failing quickly if the write never happens.
    let seen = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if skip_cache::load(tmp.path()).get("big.bin").is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        seen,
        "byte-cadence flush should write before the channel closes"
    );
    drop(tx);
    handle.await.expect("flush task join");
}

#[test]
fn safe_join_builds_nested_path_under_root() {
    let root = Path::new("/out");
    let p = safe_join(root, "assets/app.js").unwrap();
    assert_eq!(p, Path::new("/out/assets/app.js"));
}

#[test]
fn safe_join_rejects_parent_dir() {
    let err = safe_join(Path::new("/out"), "a/../../etc/passwd").unwrap_err();
    assert!(format!("{err:#}").contains(".."), "{err:#}");
}

#[test]
fn safe_join_rejects_absolute_and_empty_components() {
    // Leading slash → empty first component.
    assert!(safe_join(Path::new("/out"), "/etc/passwd").is_err());
    // Double slash → empty middle component.
    assert!(safe_join(Path::new("/out"), "a//b").is_err());
    // Trailing slash → empty last component.
    assert!(safe_join(Path::new("/out"), "a/").is_err());
    // Bare current-dir component.
    assert!(safe_join(Path::new("/out"), "./a").is_err());
    // Empty.
    assert!(safe_join(Path::new("/out"), "").is_err());
}

#[test]
fn complement_runs_gap_in_the_middle() {
    let donor = [(16384u64, 16384u64)];
    assert_eq!(
        complement_runs(&donor, 49152),
        vec![(0, 16384), (32768, 16384)]
    );
}

#[test]
fn complement_runs_empty_donor_is_the_whole_blob() {
    assert_eq!(complement_runs(&[], 49152), vec![(0, 49152)]);
}

#[test]
fn complement_runs_full_coverage_is_empty() {
    assert_eq!(complement_runs(&[(0, 49152)], 49152), Vec::new());
}

#[test]
fn complement_runs_coalesces_unsorted_overlapping_donor() {
    // Two abutting ranges covering [16384, 49152) in reverse, overlapping
    // order — coalesces to one run, leaving only the leading gap.
    let donor = [(32768u64, 16384u64), (16384u64, 16384u64)];
    assert_eq!(complement_runs(&donor, 49152), vec![(0, 16384)]);
}

#[test]
fn complement_runs_total_zero_is_empty() {
    assert_eq!(complement_runs(&[(0, 10)], 0), Vec::new());
}

const GROUP: u64 = CHUNK_GROUP_BYTES;

fn chunk(size: u64, byte: u8) -> ManifestChunk {
    ManifestChunk {
        hash: [byte; 32],
        size,
    }
}

/// A chunked entry's hints accumulate offsets by the running size sum, in
/// order, and validate that the chunk sizes reconstruct the whole-file size.
#[test]
fn hints_of_accumulates_offsets_and_validates_the_size_sum() {
    let e = ManifestEntry {
        path: "m.bin".into(),
        hash: [0xff; 32],
        size: Some(100),
        chunks: Some(vec![chunk(60, 0xaa), chunk(40, 0xbb)]),
        landed_from: None,
    };
    let hints = hints_of(&e).expect("valid hints");
    assert_eq!(hints.len(), 2);
    assert_eq!(
        (hints[0].hash, hints[0].offset, hints[0].len),
        ([0xaa; 32], 0, 60)
    );
    assert_eq!(
        (hints[1].hash, hints[1].offset, hints[1].len),
        ([0xbb; 32], 60, 40)
    );
}

/// Chunk sizes that do not sum to the whole-file size are a malformed hint set:
/// the entry falls back to a plain whole-file fetch (`None`).
#[test]
fn hints_of_rejects_a_size_sum_that_disagrees_with_the_whole_file() {
    let e = ManifestEntry {
        path: "m.bin".into(),
        hash: [0xff; 32],
        size: Some(100),
        chunks: Some(vec![chunk(60, 0xaa), chunk(30, 0xbb)]),
        landed_from: None,
    };
    assert!(hints_of(&e).is_none());
}

/// No whole-file `size` to validate against drops back to a plain fetch.
#[test]
fn hints_of_needs_a_size() {
    let no_size = ManifestEntry {
        path: "m.bin".into(),
        hash: [0xff; 32],
        size: None,
        chunks: Some(vec![chunk(60, 0xaa)]),
        landed_from: None,
    };
    assert!(hints_of(&no_size).is_none());
}

/// A materialized chunk placed at a group-aligned offset yields a donor range
/// equal to the inward-aligned hint span, read from the donor at the matching
/// source offset; the complement is `total` minus that range.
#[test]
fn plan_dedup_aligns_a_donor_inward_and_takes_the_complement() {
    let total = 3 * GROUP;
    let h = [0x11; 32];
    let mut index = HashMap::new();
    // The chunk lives at offset 0 (len one group) in the donor file.
    index.insert(
        h,
        MaterializedRange {
            source: PathBuf::from("/tmp/donorA"),
            offset: 0,
            len: GROUP,
        },
    );
    // The recipient places the same chunk at the second group.
    let hints = [Hint {
        hash: h,
        offset: GROUP,
        len: GROUP,
    }];
    let plan = plan_reassembly(&hints, &index, &FetchPlan::default(), [0; 32], total);

    assert_eq!(plan.donor.len(), 1);
    let d = &plan.donor[0];
    assert_eq!(d.dst, (GROUP, GROUP));
    assert_eq!(d.source, PathBuf::from("/tmp/donorA"));
    assert_eq!(d.src_offset, 0);
    assert_eq!(d.chunk_hash, h);
    assert_eq!((d.chunk_src_offset, d.chunk_len), (0, GROUP));
    assert_eq!(plan.drive, vec![(0, GROUP), (2 * GROUP, GROUP)]);
}

/// An unaligned hint contributes only its interior whole groups; the donor's
/// source offset tracks the inward shift, and the partial edge groups fall into
/// the complement.
#[test]
fn plan_dedup_drops_partial_edge_groups_into_the_complement() {
    let total = 4 * GROUP;
    let h = [0x22; 32];
    let mut index = HashMap::new();
    // Chunk at donor offset 5000, spanning 2*GROUP + a bit.
    index.insert(
        h,
        MaterializedRange {
            source: PathBuf::from("/tmp/donorB"),
            offset: 5000,
            len: 2 * GROUP + 500,
        },
    );
    // Recipient places it at offset 100, so [100, 100 + 2*GROUP + 500).
    let hints = [Hint {
        hash: h,
        offset: 100,
        len: 2 * GROUP + 500,
    }];
    let plan = plan_reassembly(&hints, &index, &FetchPlan::default(), [0; 32], total);

    assert_eq!(plan.donor.len(), 1);
    let d = &plan.donor[0];
    // The span [100, 100 + 2*GROUP + 500) straddles boundaries GROUP and
    // 2*GROUP, so exactly ONE whole group — [GROUP, 2*GROUP) — is inside it;
    // the sub-group head and tail fall into the complement.
    assert_eq!(d.dst, (GROUP, GROUP));
    // Source offset shifts by (GROUP - 100) from the chunk's donor start.
    assert_eq!(d.src_offset, 5000 + (GROUP - 100));
    assert_eq!(plan.drive, vec![(0, GROUP), (2 * GROUP, 2 * GROUP)]);
}

/// A chunk not in the index contributes no donor: the complement is the whole
/// blob (the plain path then handles it).
#[test]
fn plan_dedup_with_no_materialized_chunk_is_all_complement() {
    let total = 2 * GROUP;
    let hints = [Hint {
        hash: [0x33; 32],
        offset: 0,
        len: GROUP,
    }];
    let plan = plan_reassembly(
        &hints,
        &HashMap::new(),
        &FetchPlan::default(),
        [0; 32],
        total,
    );
    assert!(plan.donor.is_empty());
    assert_eq!(plan.drive, vec![(0, total)]);
}

/// A hint smaller than one chunk group (or unaligned so no whole group fits)
/// contributes no donor, even when the chunk is materialized.
#[test]
fn plan_dedup_sub_group_hint_contributes_no_donor() {
    let total = GROUP;
    let h = [0x44; 32];
    let mut index = HashMap::new();
    index.insert(
        h,
        MaterializedRange {
            source: PathBuf::from("/tmp/donorC"),
            offset: 0,
            len: 1000,
        },
    );
    let hints = [Hint {
        hash: h,
        offset: 0,
        len: 1000,
    }];
    let plan = plan_reassembly(&hints, &index, &FetchPlan::default(), [0; 32], total);
    assert!(plan.donor.is_empty());
    assert_eq!(plan.drive, vec![(0, total)]);
}

/// Review #2 (money-band): a recipient hint that names a real donor chunk hash
/// but claims a DIFFERENT length is one side lying about the chunk. It must not
/// dedup — leaving its range in the complement (paid for and bao-verified)
/// instead of trusting a placement that would run past the donor's real chunk
/// end. Without the length check `plan_dedup` produces a donor of the longer
/// claimed span, which `splice_donors` then cannot copy.
#[test]
fn plan_dedup_skips_a_donor_whose_claimed_length_disagrees() {
    let total = 2 * GROUP;
    let h = [0x55; 32];
    let mut index = HashMap::new();
    // The donor genuinely holds a ONE-group chunk under this hash.
    index.insert(
        h,
        MaterializedRange {
            source: PathBuf::from("/tmp/donorX"),
            offset: 0,
            len: GROUP,
        },
    );
    // The recipient claims the SAME hash but a longer, two-group length.
    let hints = [Hint {
        hash: h,
        offset: 0,
        len: 2 * GROUP,
    }];
    let plan = plan_reassembly(&hints, &index, &FetchPlan::default(), [0; 32], total);
    assert!(
        plan.donor.is_empty(),
        "a length-mismatched donor must not be spliced"
    );
    assert_eq!(plan.drive, vec![(0, total)]);
}

/// Review #2 (money-band): a per-donor copy that would run past the donor
/// file's end (a mismatched aligned span, a truncated or racing donor) is not a
/// hard failure — `splice_donors` queues the aligned range for a paid,
/// bao-verified re-fetch and leaves `.partial` untouched for it, rather than
/// returning `Err` and failing the whole entry.
#[test]
fn splice_donors_refetches_when_a_copy_would_run_past_the_donor_end() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor_path = tmp.path().join("donor");
    let partial = tmp.path().join("out.partial");

    // The donor holds exactly ONE group of bytes, so its chunk verifies — but
    // the donor range below asks to copy TWO groups from offset 0, which EOFs.
    let chunk_bytes: Vec<u8> = (0..GROUP)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    std::fs::write(&donor_path, &chunk_bytes).expect("write donor");
    let chunk_hash = *blake3::hash(&chunk_bytes).as_bytes();

    let total = 3 * GROUP;
    std::fs::File::create(&partial)
        .and_then(|f| f.set_len(total))
        .expect("presize partial");

    let donor = DonorRange {
        // Aligned span of TWO groups, but only one group is readable at
        // `src_offset` — the copy hits EOF.
        dst: (GROUP, 2 * GROUP),
        refetch: (GROUP, 2 * GROUP),
        source: donor_path,
        src_offset: 0,
        chunk_hash,
        chunk_src_offset: 0,
        chunk_len: GROUP,
    };

    let failed = splice_donors(&partial, std::slice::from_ref(&donor))
        .expect("a copy that EOFs must not fail the splice");
    let refetch: Vec<(u64, u64)> = failed.iter().map(|d| d.refetch).collect();
    assert_eq!(refetch, vec![(GROUP, 2 * GROUP)]);

    // Nothing was written into `.partial` for the untrusted donor — the whole
    // file stays at its pre-sized zero value, so the coming re-fetch overwrites
    // clean bytes.
    let got = std::fs::read(&partial).expect("read partial");
    assert!(
        got.iter().all(|&b| b == 0),
        "an EOF-ing donor copy must leave partial untouched"
    );
}

/// Review #1 (money-band): when a range drive COMPLETES the store — a resumed
/// run whose `.partial` already held the donor-overlap ranges, so the very first
/// complement drive finalizes and renames `<hex>.partial` -> `<hex>` —
/// `reassemble_dedup` must treat the entry as done and NOT open a `.partial`
/// that no longer exists. The [`FinalizingDriver`] simulates that finalize on
/// its first drive; without the post-drive `staging.try_exists()` guard the
/// reassembly would call `splice_donors` on the absent `.partial` and fail.
#[tokio::test]
async fn reassemble_dedup_succeeds_when_the_first_drive_finalizes_the_blob() {
    // A driver that, on its first (complement) drive, finalizes the blob by
    // creating the plain `<hex>` staging file and leaving no `.partial`.
    struct FinalizingDriver {
        hash: [u8; 32],
        staging: PathBuf,
        drives: std::cell::Cell<u32>,
    }
    impl RangeDriver for FinalizingDriver {
        fn hash(&self) -> [u8; 32] {
            self.hash
        }
        fn staging(&self) -> &Path {
            &self.staging
        }
        fn drive<'a>(
            &'a self,
            _ranges: &'a [(u64, u64)],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
            Box::pin(async move {
                self.drives.set(self.drives.get() + 1);
                std::fs::write(&self.staging, b"finalized").expect("finalize staging");
                Ok(())
            })
        }
    }

    let tmp = tempfile::tempdir().expect("tmp");
    let staging = tmp.path().join("blob");
    let driver = FinalizingDriver {
        hash: [0x11; 32],
        staging: staging.clone(),
        drives: std::cell::Cell::new(0),
    };
    // A reassembly plan with a real donor (the path is never read — the guard
    // returns before any splice) and nothing deferred.
    let plan = ReassemblePlan {
        donor: vec![DonorRange {
            dst: (GROUP, GROUP),
            refetch: (GROUP, GROUP),
            source: tmp.path().join("donor-never-read"),
            src_offset: 0,
            chunk_hash: [0x11; 32],
            chunk_src_offset: 0,
            chunk_len: GROUP,
        }],
        deferred: Vec::new(),
        drive: vec![(0, GROUP)],
    };
    let index = ChunkIndex::default();
    let fetch_plan = FetchPlan::default();

    // The store held the donor group from an earlier run.
    let resumed = [(GROUP, GROUP)];
    let res = reassemble_dedup(
        &driver,
        &plan,
        2 * GROUP,
        &resumed,
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await;

    assert!(
        res.is_ok(),
        "a first pay-now drive that finalizes the blob must succeed, not \
         fail on a missing .partial: {res:?}"
    );
    assert_eq!(driver.drives.get(), 1, "only the pay-now drive ran");
    assert!(staging.try_exists().expect("stat staging"));
    // No splice ran, so the resumed group counts as resumed, not paid (#2236).
    let outcome = res.expect("checked above");
    assert_eq!(outcome.spliced_bytes, 0);
    assert_eq!(outcome.resumed_bytes, GROUP);
    assert_eq!(
        EntryBytes::dedup(2 * GROUP, outcome),
        EntryBytes {
            paid: GROUP,
            spliced: 0,
            resumed: GROUP,
        }
    );
}

/// A deferred chunk whose whole span is spliced: `dst` is the hint's span
/// and `refetch` the groups it touches.
fn whole_deferred(hint: Hint) -> DeferredChunk {
    DeferredChunk {
        hint,
        dst: (hint.offset, hint.len),
        refetch: outward_groups((hint.offset, hint.len), u64::MAX),
    }
}

/// A chunked entry, built from `(chunk_size, chunk_hash_byte)` pairs whose sizes
/// sum to the whole-file size, with a whole-file hash of `[whole; 32]`.
fn mentry(whole: u8, size: u64, chunks: &[(u64, u8)]) -> ManifestEntry {
    ManifestEntry {
        path: format!("e{whole:02x}"),
        hash: [whole; 32],
        size: Some(size),
        chunks: Some(chunks.iter().map(|&(s, b)| chunk(s, b)).collect()),
        landed_from: None,
    }
}

/// A shared chunk is assigned to its SMALLEST containing entry, so the fetcher
/// finishes soonest; a unique chunk is left unassigned (its sole entry pays).
#[test]
fn fetch_plan_assigns_shared_chunk_to_smallest_entry() {
    let a = mentry(0x0a, 100, &[(40, 0xc0), (60, 0xc1)]);
    let b = mentry(0x0b, 300, &[(60, 0xc1), (240, 0xc2)]);
    let plan = build_fetch_plan(&[a, b]);
    assert_eq!(plan.assigned.get(&[0xc1; 32]), Some(&[0x0a; 32]));
    assert!(!plan.assigned.contains_key(&[0xc0; 32]));
    assert!(!plan.assigned.contains_key(&[0xc2; 32]));
}

/// Equal-size sharers tie-break on the whole-file hash, and the assignment is
/// independent of manifest order.
#[test]
fn fetch_plan_tie_breaks_by_whole_hash_and_is_order_independent() {
    let p1 = build_fetch_plan(&[
        mentry(0x0a, 60, &[(60, 0xc1)]),
        mentry(0x0b, 60, &[(60, 0xc1)]),
    ]);
    let p2 = build_fetch_plan(&[
        mentry(0x0b, 60, &[(60, 0xc1)]),
        mentry(0x0a, 60, &[(60, 0xc1)]),
    ]);
    assert_eq!(p1.assigned, p2.assigned);
    let winner = std::cmp::min([0x0a; 32], [0x0b; 32]);
    assert_eq!(p1.assigned.get(&[0xc1; 32]), Some(&winner));
}

/// An entry defers a chunk the plan assigns to a smaller sibling (it will splice
/// it), and drives its own unique chunk.
#[test]
fn plan_reassembly_defers_a_chunk_assigned_to_a_sibling() {
    let total = 3 * GROUP + 50_000;
    let b = mentry(0x0b, total, &[(3 * GROUP, 0x01), (50_000, 0x02)]);
    let fetch_plan = build_fetch_plan(&[
        mentry(0x0a, 3 * GROUP, &[(3 * GROUP, 0x01)]),
        mentry(0x0b, total, &[(3 * GROUP, 0x01), (50_000, 0x02)]),
    ]);
    let hints = hints_of(&b).expect("valid hints");
    let plan = plan_reassembly(&hints, &HashMap::new(), &fetch_plan, [0x0b; 32], total);
    // c1 (assigned to the smaller a) is deferred; nothing is a donor yet.
    assert_eq!(plan.deferred.len(), 1);
    assert_eq!(plan.deferred[0].hint.hash, [0x01; 32]);
    assert!(plan.donor.is_empty());
    // The drive is the complement of c1's interior — the c2 region.
    assert_eq!(plan.drive, vec![(3 * GROUP, 50_000)]);
}

/// An assigned fetcher that plans after a recipient has paid for and
/// registered its chunk (a rerun that resumes it, say) splices that chunk
/// instead of paying for it a second time: a registered chunk is a donor
/// whoever the fetch plan assigned it to.
#[test]
fn plan_reassembly_splices_its_own_assigned_chunk_once_a_sibling_registered_it() {
    let a = mentry(0x0a, 3 * GROUP, &[(3 * GROUP, 0x01)]);
    let fetch_plan = build_fetch_plan(&[
        mentry(0x0a, 3 * GROUP, &[(3 * GROUP, 0x01)]),
        mentry(
            0x0b,
            3 * GROUP + 50_000,
            &[(3 * GROUP, 0x01), (50_000, 0x02)],
        ),
    ]);
    assert_eq!(fetch_plan.assigned.get(&[0x01; 32]), Some(&[0x0a; 32]));
    let registered = HashMap::from([(
        [0x01; 32],
        MaterializedRange {
            source: PathBuf::from("b"),
            offset: 0,
            len: 3 * GROUP,
        },
    )]);
    let hints = hints_of(&a).expect("valid hints");
    let plan = plan_reassembly(&hints, &registered, &fetch_plan, [0x0a; 32], 3 * GROUP);
    assert_eq!(plan.donor.len(), 1);
    assert!(
        plan.drive.is_empty(),
        "nothing is paid twice: {:?}",
        plan.drive
    );
}

/// A shared chunk with no group-aligned interior cannot be spliced, so it is
/// paid (driven) rather than deferred, even when assigned to a sibling.
#[test]
fn plan_reassembly_pays_a_sub_group_shared_chunk_it_cannot_splice() {
    let b = mentry(0x0b, 200, &[(100, 0x09), (100, 0x08)]);
    let fetch_plan = build_fetch_plan(&[
        mentry(0x0a, 100, &[(100, 0x09)]),
        mentry(0x0b, 200, &[(100, 0x09), (100, 0x08)]),
    ]);
    let hints = hints_of(&b).expect("valid hints");
    let plan = plan_reassembly(&hints, &HashMap::new(), &fetch_plan, [0x0b; 32], 200);
    assert!(plan.deferred.is_empty());
    assert_eq!(plan.drive, vec![(0, 200)]);
}

/// Groups are scheduled smallest whole-file first (unsized last), tie-broken by
/// hash, so a shared chunk's assigned fetcher runs before its larger consumers.
#[test]
fn groups_ordered_smallest_first_stable() {
    let big = mentry(0x01, 900, &[(900, 0x91)]);
    let small = mentry(0x02, 10, &[(10, 0x92)]);
    let mid = mentry(0x03, 100, &[(100, 0x93)]);
    let refs = vec![&big, &small, &mid];
    let ordered = order_groups_smallest_first(group_by_hash(&refs));
    let sizes: Vec<_> = ordered
        .iter()
        .map(|g| g.entries.iter().find_map(|e| e.size))
        .collect();
    assert_eq!(sizes, vec![Some(10), Some(100), Some(900)]);
}

/// A group's size is any entry that declares one, not strictly the first: a
/// group whose first duplicate path is unsized but whose second is small must
/// still sort early, not last.
#[test]
fn groups_ordered_by_any_declared_size_not_just_the_first() {
    let whole = [0x42; 32];
    // Two paths for the same small blob; the first is unsized on the wire.
    let unsized_first = ManifestEntry {
        path: "a".into(),
        hash: whole,
        size: None,
        chunks: None,
        landed_from: None,
    };
    let sized_dup = ManifestEntry {
        path: "b".into(),
        hash: whole,
        size: Some(10),
        chunks: None,
        landed_from: None,
    };
    let big = mentry(0x01, 900, &[(900, 0x91)]);
    let refs = vec![&big, &unsized_first, &sized_dup];
    let ordered = order_groups_smallest_first(group_by_hash(&refs));
    let sizes: Vec<_> = ordered
        .iter()
        .map(|g| g.entries.iter().find_map(|e| e.size))
        .collect();
    assert_eq!(sizes, vec![Some(10), Some(900)]);
}

/// The run download total counts each shared chunk once (under its assigned
/// holder) and every unique byte, so a bundle that shares content downloads less
/// than its whole on-disk size; the per-blob split reports the same figures.
#[test]
fn download_bytes_counts_shared_chunks_once() {
    let a = mentry(0x0a, 3 * GROUP, &[(3 * GROUP, 0x01)]);
    let total_b = 3 * GROUP + 50_000;
    let b = mentry(0x0b, total_b, &[(3 * GROUP, 0x01), (50_000, 0x02)]);
    let fetch_plan = build_fetch_plan(&[
        mentry(0x0a, 3 * GROUP, &[(3 * GROUP, 0x01)]),
        mentry(0x0b, total_b, &[(3 * GROUP, 0x01), (50_000, 0x02)]),
    ]);

    // a downloads its whole 3*GROUP (it is c1's assigned holder); b downloads only
    // its unique c2 (50_000) and splices c1 from a.
    let empty = HashMap::new();
    let refs_a = vec![&a];
    let refs_b = vec![&b];
    let group_a = group_by_hash(&refs_a).pop().expect("group a");
    let group_b = group_by_hash(&refs_b).pop().expect("group b");
    assert_eq!(
        blob_download_reconstruct(&group_a, &fetch_plan, &empty),
        (3 * GROUP, 0)
    );
    assert_eq!(
        blob_download_reconstruct(&group_b, &fetch_plan, &empty),
        (50_000, 3 * GROUP)
    );

    // The run total is the sum: 3*GROUP + 50_000, well under the on-disk content.
    assert_eq!(
        download_bytes(&[a, b], &fetch_plan),
        Some(3 * GROUP + 50_000)
    );
}

/// A run's rate hints take this run's probe sample over the peer store,
/// fall back to the store, sort nearest first with an unknown RTT last, and
/// leave out a node with no known rate.
#[test]
fn rate_hints_of_prefer_the_run_then_the_store() -> anyhow::Result<()> {
    let key = |seed: u8| iroh::SecretKey::from_bytes(&[seed; 32]).public();
    let addr = Address::repeat_byte;
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let cfg = crate::StoreConfig::default();
    let now = crate::targets::now_secs();
    // Node 1: the store says 99 at 1 ms, the run saw 4 at 50 ms.
    store.record_sample(&key(1), 1.0, 99, now, &cfg)?;
    // Node 2: the store alone, at 20 ms.
    store.record_sample(&key(2), 20.0, 6, now, &cfg)?;
    // Node 3: no rate anywhere. Node 4: only an open, so no latency.
    store.upsert_identity(
        &NodeCandidate {
            node_id: key(4),
            eth_address: addr(4),
            region_hint: None,
            multiaddrs: alloy::primitives::Bytes::new(),
        },
        now,
    )?;
    store.record_open(&key(4), 8)?;
    let nodes = [
        (key(1), addr(1)),
        (key(2), addr(2)),
        (key(3), addr(3)),
        (key(4), addr(4)),
    ];
    let samples = HashMap::from([(key(1), (50.0, 4))]);
    let hints = rate_hints_of(&nodes, &samples, &store, now);
    let got: Vec<_> = hints.iter().map(|h| (h.provider, h.rate_per_mb)).collect();
    assert_eq!(got, vec![(addr(2), 6), (addr(1), 4), (addr(4), 8)]);
    Ok(())
}

/// The planned downloads list each distinct blob once, in first-seen order,
/// with a shared chunk under its assigned holder alone, and sum to the run's
/// download total.
#[test]
fn planned_downloads_match_the_run_total() {
    let a = mentry(0x0a, 3 * GROUP, &[(3 * GROUP, 0x01)]);
    let total_b = 3 * GROUP + 50_000;
    let b = mentry(0x0b, total_b, &[(3 * GROUP, 0x01), (50_000, 0x02)]);
    let mut a_again = a.clone();
    a_again.path = "again".into();
    let unsized_blob = ManifestEntry {
        path: "unsized".into(),
        hash: [0x0c; 32],
        size: None,
        chunks: None,
        landed_from: None,
    };
    let entries = [a.clone(), b.clone(), a_again, unsized_blob];
    let planned = planned_downloads(&entries);
    let rows: Vec<_> = planned
        .iter()
        .map(|p| (p.hash, p.size, p.download))
        .collect();
    assert_eq!(
        rows,
        vec![
            (a.hash, Some(3 * GROUP), 3 * GROUP),
            (b.hash, Some(total_b), 50_000),
            ([0x0c; 32], None, 0),
        ]
    );
    // Wire bytes carry each blob's own proof: the plain blob pays its whole
    // tree; the blob that splices its shared chunk pays for its one range
    // plus the proof path down its own tree, more than the range alone.
    let whole = decdn_bao_range::align_range_clamped(0, 3 * GROUP, 3 * GROUP)
        .unwrap()
        .wire_len();
    assert_eq!(planned[0].wire, whole);
    assert!(planned[1].wire > planned[1].download, "{:?}", planned[1]);
    assert_eq!(planned[2].wire, 0);
    let sum: u64 = planned.iter().map(|p| p.download).sum();
    assert_eq!(
        Some(sum),
        download_bytes(&entries, &build_fetch_plan(&entries))
    );
}

/// A fake [`RangeDriver`] over an in-memory `content` blob: each `drive` opens
/// the entry's ranged store the way the real drive does (`open_or_create`, so
/// a `.partial` without its `.ranges` record is truncated exactly as in
/// production), then writes the requested ranges' correct bytes into
/// `<staging>.partial`, pre-sized, never finalizing `<hex>` itself — so
/// `reassemble_dedup` exercises its splice + whole-file verify + promote path.
/// Records every driven range.
struct RecordingDriver {
    hash: [u8; 32],
    staging: PathBuf,
    content: Vec<u8>,
    driven: std::sync::Mutex<Vec<(u64, u64)>>,
}
impl RangeDriver for RecordingDriver {
    fn hash(&self) -> [u8; 32] {
        self.hash
    }
    fn staging(&self) -> &Path {
        &self.staging
    }
    fn drive<'a>(
        &'a self,
        ranges: &'a [(u64, u64)],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
        Box::pin(async move {
            use std::io::{Seek, SeekFrom, Write};
            let total = u64::try_from(self.content.len()).expect("content len fits u64");
            let (dir, stem) = ranged_store_location(&self.staging).expect("location");
            ClientRangedStore::open_or_create(&dir, &stem, self.hash, total)
                .expect("open ranged store");
            let partial = self.staging.with_extension("partial");
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&partial)
                .expect("open partial");
            f.set_len(total).expect("size partial");
            for &(off, len) in ranges {
                self.driven.lock().expect("driven lock").push((off, len));
                let start = usize::try_from(off).expect("offset fits usize");
                let end = usize::try_from(off + len).expect("end fits usize");
                f.seek(SeekFrom::Start(off)).expect("seek");
                f.write_all(&self.content[start..end]).expect("write range");
            }
            f.sync_all().expect("sync partial");
            Ok(())
        })
    }
}

/// An entry with nothing to pay for up front splices its donors into a store
/// that `ensure_partial` created, and a later fallback drive into the same
/// entry must keep those spliced bytes. A bare `.partial` with no `.ranges`
/// record would be truncated by that drive's `open_or_create`, the whole-file
/// hash would then fail, and the self-heal would pay for the whole blob.
#[tokio::test]
async fn a_fallback_drive_keeps_the_bytes_spliced_before_it() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let half = usize::try_from(2 * GROUP).expect("fits usize");

    // The first half comes from a verified on-disk donor.
    let donor_path = tmp.path().join("donor");
    std::fs::write(&donor_path, &content[..half]).expect("write donor");
    let donor_hash = *blake3::hash(&content[..half]).as_bytes();
    // The second half is deferred to a sibling that has already finished
    // without producing it, so it falls back to a paid drive.
    let deferred_hash = *blake3::hash(&content[half..]).as_bytes();

    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };
    let plan = ReassemblePlan {
        donor: vec![DonorRange {
            dst: (0, 2 * GROUP),
            refetch: (0, 2 * GROUP),
            source: donor_path,
            src_offset: 0,
            chunk_hash: donor_hash,
            chunk_src_offset: 0,
            chunk_len: 2 * GROUP,
        }],
        deferred: vec![whole_deferred(Hint {
            hash: deferred_hash,
            offset: 2 * GROUP,
            len: 2 * GROUP,
        })],
        drive: Vec::new(),
    };
    let index = ChunkIndex::default();
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(deferred_hash, [0xaa; 32]);
    index.mark_finished([0xaa; 32]);

    let outcome = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await
    .expect("reassembly must succeed");
    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert_eq!(
        driver.driven.lock().expect("driven lock").clone(),
        vec![(2 * GROUP, 2 * GROUP)],
        "only the deferred half is paid for; no self-heal re-drive"
    );
    assert_eq!(outcome.spliced_bytes, 2 * GROUP);
    assert_eq!(outcome.resumed_bytes, 0);
}

/// An entry resumed from an earlier run's `.partial` whose splice then
/// covers part of that prefix: the overlap counts once, as spliced, and only
/// the bytes neither resumed nor spliced count as paid (#2236).
#[tokio::test]
async fn a_splice_over_a_resumed_prefix_counts_the_overlap_as_spliced() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let half = usize::try_from(2 * GROUP).expect("fits usize");

    // An earlier, interrupted run fetched `[0, 3*GROUP)` into the store.
    let staging = tmp.path().join("blob");
    ClientRangedStore::seed_checkpointed_prefix(tmp.path(), "blob", &content, 3 * GROUP)
        .expect("seed the resume record");
    let resumed = resumed_spans(&staging, whole);
    assert_eq!(resumed, vec![(0, 3 * GROUP)]);

    // This run splices `[0, 2*GROUP)` from a donor and pays for the deferred
    // `[2*GROUP, 4*GROUP)` when its sibling finishes without producing it.
    let donor_path = tmp.path().join("donor");
    std::fs::write(&donor_path, &content[..half]).expect("write donor");
    let donor_hash = *blake3::hash(&content[..half]).as_bytes();
    let deferred_hash = *blake3::hash(&content[half..]).as_bytes();
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };
    let plan = ReassemblePlan {
        donor: vec![DonorRange {
            dst: (0, 2 * GROUP),
            refetch: (0, 2 * GROUP),
            source: donor_path,
            src_offset: 0,
            chunk_hash: donor_hash,
            chunk_src_offset: 0,
            chunk_len: 2 * GROUP,
        }],
        deferred: vec![whole_deferred(Hint {
            hash: deferred_hash,
            offset: 2 * GROUP,
            len: 2 * GROUP,
        })],
        drive: Vec::new(),
    };
    let index = ChunkIndex::default();
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(deferred_hash, [0xaa; 32]);
    index.mark_finished([0xaa; 32]);

    let outcome = reassemble_dedup(
        &driver,
        &plan,
        total,
        &resumed,
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await
    .expect("reassembly must succeed");
    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert_eq!(
        outcome.spliced_bytes,
        2 * GROUP,
        "the splice keeps priority"
    );
    assert_eq!(outcome.resumed_bytes, GROUP, "only the unspliced prefix");
    assert_eq!(
        EntryBytes::dedup(total, outcome),
        EntryBytes {
            paid: GROUP,
            spliced: 2 * GROUP,
            resumed: GROUP,
        }
    );
}

/// A self-heal whole-blob re-drive discards every splice, so the resumed
/// prefix counts whole: the store still holds it, and the re-drive does not
/// fetch it again.
#[test]
fn a_self_heal_outcome_counts_the_whole_resumed_prefix() {
    let outcome = DedupOutcome::of(&[], &[(0, 3 * GROUP), (GROUP, GROUP)], 4 * GROUP, 2);
    assert_eq!(outcome.spliced_bytes, 0);
    assert_eq!(outcome.resumed_bytes, 3 * GROUP);
    assert_eq!(EntryBytes::dedup(4 * GROUP, outcome).paid, GROUP);
}

/// The whole-blob path reads what an earlier run fetched from the resume
/// record and pays only for the rest (#2236). A fresh entry and an
/// unreadable record resume nothing.
#[test]
fn resumed_spans_read_the_record_an_earlier_run_left() {
    let tmp = tempfile::tempdir().expect("tmp");
    let content: Vec<u8> = (0..3 * GROUP + 99)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let len = u64::try_from(content.len()).expect("fits u64");
    let hash = *blake3::hash(&content).as_bytes();
    let staging = tmp.path().join("blob");

    assert!(
        resumed_spans(&staging, hash).is_empty(),
        "no record, no spans"
    );

    ClientRangedStore::seed_checkpointed_prefix(tmp.path(), "blob", &content, GROUP)
        .expect("seed the resume record");
    let resumed = resumed_spans(&staging, hash);
    assert_eq!(resumed, vec![(0, GROUP)]);
    assert_eq!(
        EntryBytes::whole_blob(len, span_bytes(&resumed)),
        EntryBytes {
            paid: len - GROUP,
            spliced: 0,
            resumed: GROUP,
        }
    );
    // A resumed count never exceeds the blob.
    assert_eq!(EntryBytes::whole_blob(10, 20).paid, 0);
    assert_eq!(EntryBytes::whole_blob(10, 20).resumed, 10);

    std::fs::write(tmp.path().join("blob.partial.ranges"), b"not json").expect("corrupt");
    assert!(
        resumed_spans(&staging, hash).is_empty(),
        "an unreadable record resumes nothing"
    );
}

/// Two donor chunks of a blob, `[0, cut)` and `[cut, 4*GROUP)`, each written
/// to its own donor file, with the index entries a plan resolves them by.
/// `cut` is not a chunk-group boundary.
fn two_donor_fixture(
    dir: &Path,
    content: &[u8],
    cut: u64,
) -> (Vec<Hint>, HashMap<[u8; 32], MaterializedRange>) {
    let c = usize::try_from(cut).expect("fits usize");
    let total = u64::try_from(content.len()).expect("fits u64");
    let mut hints = Vec::new();
    let mut index = HashMap::new();
    for (name, bytes, offset) in [("a", &content[..c], 0), ("b", &content[c..], cut)] {
        let path = dir.join(name);
        std::fs::write(&path, bytes).expect("write donor");
        let hash = *blake3::hash(bytes).as_bytes();
        let len = u64::try_from(bytes.len()).expect("fits u64");
        hints.push(Hint { hash, offset, len });
        index.insert(
            hash,
            MaterializedRange {
                source: path,
                offset: 0,
                len,
            },
        );
    }
    assert_eq!(hints.iter().map(|h| h.len).sum::<u64>(), total);
    (hints, index)
}

/// Two adjacent donor chunks that meet inside a chunk group form one spliced
/// run: the boundary group is spliced from both sides, so nothing is driven.
#[test]
fn plan_dedup_adjacent_donors_at_an_unaligned_boundary_leave_no_drive_gap() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content = vec![7u8; usize::try_from(total).expect("fits usize")];
    let cut = GROUP + 5000;
    let (hints, index) = two_donor_fixture(tmp.path(), &content, cut);

    let plan = plan_reassembly(&hints, &index, &FetchPlan::default(), [0; 32], total);

    assert!(plan.drive.is_empty(), "drive: {:?}", plan.drive);
    let dsts: Vec<(u64, u64)> = plan.donor.iter().map(|d| d.dst).collect();
    assert_eq!(dsts, vec![(0, cut), (cut, total - cut)]);
    assert!(plan.donor.iter().all(|d| d.src_offset == 0));
    assert_eq!(plan.donor[1].refetch, (GROUP, total - GROUP));
}

/// A donor next to a chunk that must be driven splices only up to the last
/// group boundary inside it: the shared group is driven once, with the
/// driven chunk.
#[test]
fn plan_dedup_donor_next_to_a_driven_chunk_drives_the_shared_group_once() {
    let total = 3 * GROUP;
    let cut = GROUP + 5000;
    let donor_hash = [0x44; 32];
    let mut index = HashMap::new();
    index.insert(
        donor_hash,
        MaterializedRange {
            source: PathBuf::from("/tmp/donor"),
            offset: 0,
            len: cut,
        },
    );
    let hints = [
        Hint {
            hash: donor_hash,
            offset: 0,
            len: cut,
        },
        Hint {
            hash: [0x45; 32],
            offset: cut,
            len: total - cut,
        },
    ];

    let plan = plan_reassembly(&hints, &index, &FetchPlan::default(), [0; 32], total);

    let dsts: Vec<(u64, u64)> = plan.donor.iter().map(|d| d.dst).collect();
    assert_eq!(dsts, vec![(0, GROUP)]);
    assert_eq!(plan.drive, vec![(GROUP, 2 * GROUP)]);
}

/// A donor chunk that meets a deferred chunk inside a group also forms one
/// run with it: neither side's partial group is driven.
#[test]
fn plan_dedup_donor_meeting_a_deferred_chunk_leaves_no_drive_gap() {
    let total = 3 * GROUP;
    let cut = GROUP + 5000;
    let (donor_hash, deferred_hash) = ([0x46; 32], [0x47; 32]);
    let mut index = HashMap::new();
    index.insert(
        donor_hash,
        MaterializedRange {
            source: PathBuf::from("/tmp/donor"),
            offset: 0,
            len: cut,
        },
    );
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(deferred_hash, [0xaa; 32]);
    let hints = [
        Hint {
            hash: donor_hash,
            offset: 0,
            len: cut,
        },
        Hint {
            hash: deferred_hash,
            offset: cut,
            len: total - cut,
        },
    ];

    let plan = plan_reassembly(&hints, &index, &fetch_plan, [0x0b; 32], total);

    assert!(plan.drive.is_empty(), "drive: {:?}", plan.drive);
    assert_eq!(plan.donor[0].dst, (0, cut));
    assert_eq!(plan.deferred[0].dst, (cut, total - cut));
    assert_eq!(plan.deferred[0].refetch, (GROUP, 2 * GROUP));
}

#[test]
fn spliced_runs_merge_touching_spans_and_round_only_the_run_ends_inward() {
    let total = 10 * GROUP;
    // Two touching spans that meet mid-group, and one apart from them.
    let spans = [
        (100, GROUP),
        (GROUP + 100, 2 * GROUP),
        (5 * GROUP + 1, 2 * GROUP),
    ];
    assert_eq!(
        spliced_runs(&spans, total),
        vec![(GROUP, 3 * GROUP), (6 * GROUP, 7 * GROUP)]
    );
    // A run with no whole group inside it is dropped.
    assert!(spliced_runs(&[(1, GROUP)], total).is_empty());
}

/// The blob end counts as a group boundary: a run that reaches it keeps its
/// final partial group.
#[test]
fn spliced_runs_keep_the_blob_end() {
    let total = 2 * GROUP + 700;
    assert_eq!(
        spliced_runs(&[(GROUP, GROUP + 700)], total),
        vec![(GROUP, total)]
    );
}

/// Adjacent donors meeting mid-group reassemble the blob with nothing paid for.
#[tokio::test]
async fn reassemble_dedup_splices_a_boundary_group_from_two_donors() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let (hints, map) = two_donor_fixture(tmp.path(), &content, GROUP + 5000);
    let plan = plan_reassembly(&hints, &map, &FetchPlan::default(), whole, total);
    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };

    let outcome = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &ChunkIndex::default(),
        &FetchPlan::default(),
        None,
        &|| {},
    )
    .await
    .expect("reassembly must succeed");

    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert!(driver.driven.lock().expect("driven lock").is_empty());
    assert_eq!(outcome.spliced_bytes, total);
}

/// A donor that fails its verification re-hash is fetched again over every
/// group its span touches — including the boundary group it shares with a
/// good neighbour — and the blob still reassembles byte-exact.
#[tokio::test]
async fn reassemble_dedup_refetches_the_outward_group_span_of_a_failed_donor() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let cut = GROUP + 5000;
    let (hints, map) = two_donor_fixture(tmp.path(), &content, cut);
    let plan = plan_reassembly(&hints, &map, &FetchPlan::default(), whole, total);
    // Corrupt donor B on disk after planning: its chunk no longer re-hashes.
    std::fs::write(tmp.path().join("b"), vec![0u8; 16]).expect("corrupt donor b");
    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };

    let outcome = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &ChunkIndex::default(),
        &FetchPlan::default(),
        None,
        &|| {},
    )
    .await
    .expect("reassembly must succeed");

    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert_eq!(
        driver.driven.lock().expect("driven lock").clone(),
        vec![(GROUP, total - GROUP)],
        "the failed donor's whole groups are driven, boundary group included"
    );
    // The drive wrote and paid for donor A's share of the boundary group.
    assert_eq!(outcome.spliced_bytes, GROUP);
    assert_eq!(outcome.hints_ignored, 1);
}

/// A deferred chunk whose fetcher finishes without producing it is driven
/// over every group its span touches, so the boundary group it shares with a
/// spliced donor ends up whole.
#[tokio::test]
async fn reconcile_deferred_falls_back_to_the_outward_group_span() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let cut = GROUP + 5000;
    let (hints, mut map) = two_donor_fixture(tmp.path(), &content, cut);
    // Chunk b is not materialized: it is assigned to a sibling that has
    // already finished without registering it.
    let b_hash = hints[1].hash;
    map.remove(&b_hash);
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(b_hash, [0xaa; 32]);
    let index = ChunkIndex::default();
    index.mark_finished([0xaa; 32]);
    let plan = plan_reassembly(&hints, &map, &fetch_plan, whole, total);
    assert!(plan.drive.is_empty(), "drive: {:?}", plan.drive);
    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };

    let outcome = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await
    .expect("reassembly must succeed");

    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert_eq!(
        driver.driven.lock().expect("driven lock").clone(),
        vec![(GROUP, total - GROUP)]
    );
    // The fallback drive wrote and paid for donor A's share of the boundary group.
    assert_eq!(outcome.spliced_bytes, GROUP);
}

/// A re-fetch of a failed donor at the first splice can cover part of a
/// deferred chunk that the tail splices later. The overlap is paid, so it
/// does not count as spliced.
#[tokio::test]
async fn spliced_bytes_exclude_a_first_splice_refetch_under_a_tail_splice() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let cut = GROUP + 5000;
    let (hints, mut map) = two_donor_fixture(tmp.path(), &content, cut);
    // Chunk a is deferred to a sibling that has already registered it.
    let a_hash = hints[0].hash;
    map.remove(&a_hash);
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(a_hash, [0xaa; 32]);
    let plan = plan_reassembly(&hints, &map, &fetch_plan, whole, total);
    assert_eq!(plan.deferred.len(), 1);
    assert_eq!(plan.donor.len(), 1);
    let index = ChunkIndex::default();
    index.register(Some(&hints[..1]), &tmp.path().join("a"));
    // Donor b fails its re-hash, so the first splice re-fetches its groups.
    std::fs::write(tmp.path().join("b"), vec![0u8; 16]).expect("corrupt donor b");
    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };

    let outcome = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await
    .expect("reassembly must succeed");

    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert_eq!(
        driver.driven.lock().expect("driven lock").clone(),
        vec![(GROUP, total - GROUP)]
    );
    assert_eq!(outcome.spliced_bytes, GROUP);
    assert_eq!(outcome.hints_ignored, 1);
}

/// The bytes of `spliced` that no `driven` span covers, over `[0, total)`.
fn net_spliced(spliced: &[(u64, u64)], driven: &[(u64, u64)], total: u64) -> u64 {
    span_bytes(&uncovered_runs(spliced, driven, total))
}

#[test]
fn uncovered_runs_keeps_the_uncovered_parts_as_disjoint_spans() {
    assert_eq!(
        uncovered_runs(&[(300, 100), (0, 100)], &[(50, 100), (350, 10)], 1000),
        vec![(0, 50), (300, 50), (360, 40)]
    );
    // Overlapping inputs coalesce, and the result clamps to `total`.
    assert_eq!(
        uncovered_runs(&[(0, 60), (40, 100)], &[], 100),
        vec![(0, 100)]
    );
    assert!(uncovered_runs(&[(0, 100)], &[(0, 100)], 1000).is_empty());
}

#[test]
fn net_spliced_subtracts_only_the_driven_overlap() {
    // Disjoint: nothing driven under the splice.
    assert_eq!(net_spliced(&[(0, 100)], &[(200, 50)], 1000), 100);
    // Nested: the drive sits inside the splice.
    assert_eq!(net_spliced(&[(0, 100)], &[(20, 30)], 1000), 70);
    // The drive covers the whole splice.
    assert_eq!(net_spliced(&[(10, 20)], &[(0, 100)], 1000), 0);
    // Touching spans share no byte.
    assert_eq!(net_spliced(&[(0, 100)], &[(100, 100)], 1000), 100);
    // Overlapping drives count once, across several splices, in any order.
    assert_eq!(
        net_spliced(
            &[(300, 100), (0, 100)],
            &[(50, 100), (60, 10), (350, 100)],
            1000
        ),
        100
    );
    assert_eq!(net_spliced(&[], &[(0, 10)], 1000), 0);
    assert_eq!(net_spliced(&[(0, 10)], &[], 1000), 10);
}

/// A deferred chunk whose assigned fetcher registers its donor only AFTER the
/// consumer has started is still spliced at the tail — never driven — so the
/// shared bytes are paid for once (by the sibling) and copied here from disk.
#[tokio::test]
async fn reconcile_deferred_splices_a_late_registered_donor() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();

    // Deferred chunk = the first two groups; its hash is what the donor must
    // re-hash to. The consumer's own pay-now range is the last two groups.
    let deferred_len = 2 * GROUP;
    let dl = usize::try_from(deferred_len).expect("fits usize");
    let chunk_hash = *blake3::hash(&content[..dl]).as_bytes();

    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };
    let plan = ReassemblePlan {
        donor: Vec::new(),
        deferred: vec![whole_deferred(Hint {
            hash: chunk_hash,
            offset: 0,
            len: deferred_len,
        })],
        drive: vec![(deferred_len, 2 * GROUP)],
    };
    let index = std::sync::Arc::new(ChunkIndex::default());
    let mut fetch_plan = FetchPlan::default();
    // The deferred chunk is assigned to some sibling whole hash.
    fetch_plan.assigned.insert(chunk_hash, [0xaa; 32]);

    // Register the donor a moment after the consumer starts, simulating a
    // sibling finishing mid-run.
    let donor_path = tmp.path().join("donor");
    std::fs::write(&donor_path, &content[..dl]).expect("write donor");
    let index_bg = index.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        index_bg.register(
            Some(&[Hint {
                hash: chunk_hash,
                offset: 0,
                len: deferred_len,
            }]),
            &donor_path,
        );
    });

    let res = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await;
    assert!(res.is_ok(), "reassembly must succeed: {res:?}");
    assert!(staging.try_exists().expect("stat staging"));
    let got = std::fs::read(&staging).expect("read staging");
    assert_eq!(
        got, content,
        "reassembled blob must match the whole content"
    );
    // The deferred range was spliced, never driven.
    let driven_ranges = driver.driven.lock().expect("driven lock").clone();
    assert!(
        driven_ranges.iter().all(|&(off, _)| off >= deferred_len),
        "the deferred range must be spliced, not driven: {driven_ranges:?}"
    );
}

/// When a deferred chunk's assigned fetcher FINISHES without registering it (a
/// failed fetcher), the tail reconcile drives (pays for) the range itself, so
/// the entry still completes — liveness, never a hang.
#[tokio::test]
async fn reconcile_deferred_pays_when_the_assigned_fetcher_never_produces_it() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let deferred_len = 2 * GROUP;
    let dl = usize::try_from(deferred_len).expect("fits usize");
    let chunk_hash = *blake3::hash(&content[..dl]).as_bytes();

    let staging = tmp.path().join("blob");
    let driver = RecordingDriver {
        hash: whole,
        staging: staging.clone(),
        content: content.clone(),
        driven: std::sync::Mutex::new(Vec::new()),
    };
    let plan = ReassemblePlan {
        donor: Vec::new(),
        deferred: vec![whole_deferred(Hint {
            hash: chunk_hash,
            offset: 0,
            len: deferred_len,
        })],
        drive: vec![(deferred_len, 2 * GROUP)],
    };
    let index = std::sync::Arc::new(ChunkIndex::default());
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(chunk_hash, [0xaa; 32]);

    // The assigned fetcher finishes shortly, producing nothing.
    let index_bg = index.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        index_bg.mark_finished([0xaa; 32]);
    });

    let res = reassemble_dedup(
        &driver,
        &plan,
        total,
        &[],
        None,
        &index,
        &fetch_plan,
        None,
        &|| {},
    )
    .await;
    assert!(res.is_ok(), "reassembly must succeed via fallback: {res:?}");
    let got = std::fs::read(&staging).expect("read staging");
    assert_eq!(got, content);
    // The deferred range was driven (paid) as a fallback.
    let driven_ranges = driver.driven.lock().expect("driven lock").clone();
    assert!(
        driven_ranges
            .iter()
            .any(|&(off, len)| off == 0 && len == deferred_len),
        "the deferred range must be driven as a fallback: {driven_ranges:?}"
    );
}

/// A [`RecordingDriver`] that drives under a `--jobs` [`FetchSlot`], as the
/// production driver does, and records whether the slot was held for each
/// drive.
struct SlotDriver<'s, 'g> {
    inner: RecordingDriver,
    slot: &'s FetchSlot<'g>,
    held_on_drive: std::sync::Mutex<Vec<bool>>,
}
impl RangeDriver for SlotDriver<'_, '_> {
    fn hash(&self) -> [u8; 32] {
        self.inner.hash()
    }
    fn staging(&self) -> &Path {
        self.inner.staging()
    }
    fn drive<'a>(
        &'a self,
        ranges: &'a [(u64, u64)],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
        Box::pin(async move {
            self.slot.ensure().await?;
            self.held_on_drive
                .lock()
                .expect("held lock")
                .push(self.slot.held());
            self.inner.drive(ranges).await
        })
    }
    fn release_slot(&self) {
        self.slot.release();
    }
    fn retake_slot(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + '_>> {
        Box::pin(self.slot.ensure())
    }
}

/// #2283: an entry whose complement has landed and that only waits for a
/// sibling's donor chunks gives its `--jobs` slot to a queued entry, and
/// takes one again for the fallback drive when the sibling finishes
/// without them. With one slot, a waiter that kept it would starve the
/// queued entry until the wait ended.
#[tokio::test]
async fn a_donor_waiter_gives_its_slot_to_a_queued_entry_and_retakes_it_to_drive() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let deferred_len = 2 * GROUP;
    let dl = usize::try_from(deferred_len).expect("fits usize");
    let chunk_hash = *blake3::hash(&content[..dl]).as_bytes();

    let gate = JobGate::new(1);
    let slot = FetchSlot::acquire(&gate).await.expect("first slot");
    let staging = tmp.path().join("blob");
    let driver = SlotDriver {
        inner: RecordingDriver {
            hash: whole,
            staging: staging.clone(),
            content: content.clone(),
            driven: std::sync::Mutex::new(Vec::new()),
        },
        slot: &slot,
        held_on_drive: std::sync::Mutex::new(Vec::new()),
    };
    let plan = ReassemblePlan {
        donor: Vec::new(),
        deferred: vec![whole_deferred(Hint {
            hash: chunk_hash,
            offset: 0,
            len: deferred_len,
        })],
        drive: vec![(deferred_len, 2 * GROUP)],
    };
    let index = ChunkIndex::default();
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(chunk_hash, [0xaa; 32]);

    // The queued entry: it waits for the one slot, holds it briefly, then
    // its sibling (the assigned fetcher) finishes without the chunk.
    let queued = async {
        let got = tokio::time::timeout(std::time::Duration::from_secs(10), gate.permit())
            .await
            .is_ok_and(|permit| permit.is_ok());
        index.mark_finished([0xaa; 32]);
        got
    };
    let (res, queued_ran) = tokio::join!(
        reassemble_dedup(
            &driver,
            &plan,
            total,
            &[],
            None,
            &index,
            &fetch_plan,
            None,
            &|| {},
        ),
        queued
    );
    assert!(res.is_ok(), "reassembly must succeed via fallback: {res:?}");
    assert!(
        queued_ran,
        "the waiter must give its slot to the queued entry"
    );
    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    let held = driver.held_on_drive.lock().expect("held lock").clone();
    assert_eq!(
        held,
        vec![true, true],
        "the complement drive and the fallback drive each run under a slot"
    );
    assert!(slot.held(), "the entry ends holding its slot again");
}

/// #2283: a waiter whose donor arrives needs no fallback drive, yet it
/// takes its slot back as the wait ends: the splice, the whole-file hash and
/// the materialize after it are heavy disk work `--jobs` bounds too.
#[tokio::test]
async fn a_waiter_whose_donor_arrives_holds_its_slot_again_for_the_splice() {
    let tmp = tempfile::tempdir().expect("tmp");
    let total = 4 * GROUP;
    let content: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let whole = *blake3::hash(&content).as_bytes();
    let deferred_len = 2 * GROUP;
    let dl = usize::try_from(deferred_len).expect("fits usize");
    let chunk_hash = *blake3::hash(&content[..dl]).as_bytes();
    let hint = Hint {
        hash: chunk_hash,
        offset: 0,
        len: deferred_len,
    };

    let gate = JobGate::new(1);
    let slot = FetchSlot::acquire(&gate).await.expect("first slot");
    let staging = tmp.path().join("blob");
    let driver = SlotDriver {
        inner: RecordingDriver {
            hash: whole,
            staging: staging.clone(),
            content: content.clone(),
            driven: std::sync::Mutex::new(Vec::new()),
        },
        slot: &slot,
        held_on_drive: std::sync::Mutex::new(Vec::new()),
    };
    let plan = ReassemblePlan {
        donor: Vec::new(),
        deferred: vec![whole_deferred(hint)],
        drive: vec![(deferred_len, 2 * GROUP)],
    };
    let index = ChunkIndex::default();
    let mut fetch_plan = FetchPlan::default();
    fetch_plan.assigned.insert(chunk_hash, [0xaa; 32]);
    let donor_path = tmp.path().join("donor");
    std::fs::write(&donor_path, &content[..dl]).expect("write donor");

    // The sibling fetcher takes the freed slot, registers the donor, and
    // gives the slot back as it ends.
    let sibling = async {
        let permit = gate.permit().await.expect("the waiter freed its slot");
        index.register(Some(&[hint]), &donor_path);
        drop(permit);
    };
    let (res, ()) = tokio::join!(
        reassemble_dedup(
            &driver,
            &plan,
            total,
            &[],
            None,
            &index,
            &fetch_plan,
            None,
            &|| {},
        ),
        sibling
    );
    assert!(res.is_ok(), "reassembly must succeed: {res:?}");
    assert_eq!(std::fs::read(&staging).expect("read staging"), content);
    assert_eq!(
        driver.held_on_drive.lock().expect("held lock").len(),
        1,
        "only the complement was driven: the donor was spliced"
    );
    assert!(slot.held(), "the entry holds its slot again after the wait");
}

/// Review #3 (money-band): the run-end sweep removes a normal donor staging
/// blob but KEEPS one `mark_retained` flagged (its group failed to
/// materialize), so a rerun resumes from the finalized `<hex>` instead of
/// re-paying the whole blob.
#[tokio::test]
async fn run_end_sweep_keeps_a_retained_donor_and_removes_a_normal_one() {
    let tmp = tempfile::tempdir().expect("tmp");
    let normal = tmp.path().join("normal");
    let retained = tmp.path().join("retained");
    std::fs::write(&normal, b"n").expect("write normal");
    std::fs::write(&retained, b"r").expect("write retained");

    let index = ChunkIndex::default();
    index.register(
        Some(&[Hint {
            hash: [1; 32],
            offset: 0,
            len: 1,
        }]),
        &normal,
    );
    index.register(
        Some(&[Hint {
            hash: [2; 32],
            offset: 0,
            len: 1,
        }]),
        &retained,
    );
    // The retained donor's blob is paid for but its materialize failed.
    index.mark_retained(&retained);

    sweep_donor_sources(&index).await;

    assert!(
        !normal.exists(),
        "a normal donor source is swept at run end"
    );
    assert!(
        retained.exists(),
        "a retained (materialize-failed) donor source survives the sweep"
    );
}

/// A [`ChunkIndex::seed_disk`] source is an on-disk OUTPUT file, not a
/// staging blob: [`ChunkIndex::sources`] (the run-end sweep's deletion list)
/// must exclude it, or a materialized output would be deleted. It is still
/// resolvable as a splice donor for planning.
#[test]
fn chunk_index_seeded_source_survives_sweep() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().join("kept.bin");
    std::fs::write(&out, b"donorbytes").expect("write");
    let idx = ChunkIndex::default();
    idx.seed_disk([7u8; 32], &out, 0, 10);
    // A seeded output path is NOT a sweepable staging source.
    assert!(idx.sources().is_empty());
    // But it IS resolvable as a donor for planning.
    let guard = idx.map.lock().unwrap_or_else(PoisonError::into_inner);
    assert!(guard.contains_key(&[7u8; 32]));
}

/// `splice_donors` happy path: a donor file holds a verified chunk at some
/// offset (with padding on both sides), and the aligned subset lands at the
/// correct recipient offset in `.partial` — nothing else in `.partial` is
/// touched, and nothing is queued for refetch.
#[test]
fn splice_donors_copies_a_verified_subset_into_partial() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor_path = tmp.path().join("donor");
    let partial = tmp.path().join("out.partial");

    // The chunk occupies exactly one group, padded on both sides in the donor
    // file so the source offset is not trivially zero.
    let chunk_bytes: Vec<u8> = (0..GROUP)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let pad_before = 100usize;
    let mut donor_data = vec![0xEEu8; pad_before];
    donor_data.extend_from_slice(&chunk_bytes);
    donor_data.extend_from_slice(&[0xEE; 50]);
    std::fs::write(&donor_path, &donor_data).expect("write donor");

    let chunk_hash = *blake3::hash(&chunk_bytes).as_bytes();
    let total = 2 * GROUP;
    // splice_donors only opens `partial` for write, so pre-size it (zeros)
    // the way the ranged store would before splicing runs.
    std::fs::File::create(&partial)
        .and_then(|f| f.set_len(total))
        .expect("presize partial");

    let donor = DonorRange {
        dst: (GROUP, GROUP),
        refetch: (GROUP, GROUP),
        source: donor_path,
        src_offset: u64::try_from(pad_before).expect("pad_before fits in u64"),
        chunk_hash,
        chunk_src_offset: u64::try_from(pad_before).expect("pad_before fits in u64"),
        chunk_len: GROUP,
    };

    let refetch = splice_donors(&partial, std::slice::from_ref(&donor)).expect("splice");
    assert!(refetch.is_empty(), "{refetch:?}");

    let got = std::fs::read(&partial).expect("read partial");
    let g = usize::try_from(GROUP).expect("GROUP fits in usize");
    assert_eq!(&got[g..2 * g], &chunk_bytes[..], "spliced subset mismatch");
    assert!(
        got[..g].iter().all(|&b| b == 0),
        "untouched region must stay zero"
    );
}

/// A donor whose recorded chunk bytes no longer hash to the hint's chunk hash
/// (corruption, or a stale/overwritten donor file) is never trusted: its range
/// is pushed to the refetch list and no bytes are written into `.partial` for
/// it.
#[test]
fn splice_donors_refetches_a_corrupted_donor_chunk() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor_path = tmp.path().join("donor");
    let partial = tmp.path().join("out.partial");

    let chunk_bytes: Vec<u8> = (0..GROUP)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let chunk_hash = *blake3::hash(&chunk_bytes).as_bytes();

    // Write a CORRUPTED copy of the chunk to the donor file (flip one byte),
    // so the on-disk bytes no longer match `chunk_hash`.
    let mut corrupted = chunk_bytes.clone();
    let mid = corrupted.len() / 2;
    corrupted[mid] ^= 0xFF;
    std::fs::write(&donor_path, &corrupted).expect("write corrupted donor");

    let total = 2 * GROUP;
    std::fs::File::create(&partial)
        .and_then(|f| f.set_len(total))
        .expect("presize partial");

    let donor = DonorRange {
        dst: (GROUP, GROUP),
        refetch: (GROUP, GROUP),
        source: donor_path,
        src_offset: 0,
        chunk_hash,
        chunk_src_offset: 0,
        chunk_len: GROUP,
    };

    let failed = splice_donors(&partial, std::slice::from_ref(&donor)).expect("splice");
    let refetch: Vec<(u64, u64)> = failed.iter().map(|d| d.refetch).collect();
    assert_eq!(refetch, vec![donor.refetch]);

    // No (wrong) bytes were written for the untrusted donor: the recipient
    // range stays at its pre-sized zero value.
    let got = std::fs::read(&partial).expect("read partial");
    let g = usize::try_from(GROUP).expect("GROUP fits in usize");
    assert!(
        got[g..2 * g].iter().all(|&b| b == 0),
        "untrusted donor must not write into partial"
    );
}

/// `chunk_verified` is a straightforward hash-match gate: true for bytes that
/// hash to `expected`, false for a mismatch — the boundary condition
/// `splice_donors` relies on to decide trust.
#[test]
fn chunk_verified_matches_true_and_false() {
    let tmp = tempfile::tempdir().expect("tmp");
    let p = tmp.path().join("f");
    let data = vec![9u8; 2000];
    std::fs::write(&p, &data).expect("write");
    let hash = *blake3::hash(&data).as_bytes();

    let mut f = std::fs::File::open(&p).expect("open");
    assert!(chunk_verified(&mut f, 0, 2000, hash));

    let mut f2 = std::fs::File::open(&p).expect("open");
    assert!(!chunk_verified(&mut f2, 0, 2000, [0u8; 32]));
}

/// `copy_exact` copies precisely the requested length, no more, from the
/// source's current position to the destination's current position.
#[test]
fn copy_exact_copies_only_the_requested_length() {
    let tmp = tempfile::tempdir().expect("tmp");
    let src_path = tmp.path().join("src");
    let out_path = tmp.path().join("out");
    std::fs::write(&src_path, b"hello world").expect("write src");
    std::fs::write(&out_path, []).expect("write out");

    let mut src = std::fs::File::open(&src_path).expect("open src");
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .open(&out_path)
        .expect("open out");
    copy_exact(&mut src, &mut out, 5).expect("copy_exact");
    drop(out);

    let got = std::fs::read(&out_path).expect("read out");
    assert_eq!(&got[..], b"hello");
}

/// `hash_partial` streams the whole file and returns its BLAKE3 hash, matching
/// a direct in-memory hash of the same bytes.
#[test]
fn hash_partial_matches_direct_blake3_of_file_contents() {
    let tmp = tempfile::tempdir().expect("tmp");
    let p = tmp.path().join("f");
    let data: Vec<u8> = (0..5000u32)
        .map(|i| u8::try_from(i % 256).expect("i % 256 fits in u8"))
        .collect();
    std::fs::write(&p, &data).expect("write");

    let got = hash_partial(&p).expect("hash_partial");
    assert_eq!(got, *blake3::hash(&data).as_bytes());
}

#[test]
fn staging_path_is_the_plain_hex_final_blob_not_a_partial() {
    let tmp = tempfile::tempdir().expect("tmp");
    let hash = [0xabu8; 32];
    let p = staging_path(tmp.path(), hash).expect("staging_path");
    let name = p.file_name().and_then(|n| n.to_str()).expect("name");
    assert!(
        !name.ends_with(".partial"),
        "staging file must be the plain finalized blob, got {name}"
    );
    assert_eq!(name, blake3::Hash::from_bytes(hash).to_hex().to_string());
    assert!(p.starts_with(tmp.path().join(STAGING_DIR)));
}

/// [`BundleManifest::parse`] of the JSON `manifest`.
fn parse_json(manifest: &serde_json::Value) -> anyhow::Result<BundleManifest> {
    BundleManifest::parse(manifest.to_string().as_bytes())
}

#[test]
fn parse_manifest_accepts_v1_with_optional_size() {
    let m = parse_json(&serde_json::json!({"version": 1, "entries": [
        {"path": "a.txt", "hash": format_hash([0xab; 32]), "size": 4},
        {"path": "b", "hash": format_hash([0xcd; 32])},
    ]}))
    .unwrap();
    assert_eq!(m.entries.len(), 2);
    assert_eq!(m.entries[0].hash, [0xab; 32]);
    assert_eq!(m.entries[0].size, Some(4));
    assert_eq!(m.entries[1].size, None);
}

/// A parsed manifest's hash is its bytes' BLAKE3: the hash its URI names.
#[test]
fn parse_manifest_keeps_the_blob_hash() {
    let json = br#"{"version":1,"entries":[]}"#;
    let m = BundleManifest::parse(json).unwrap();
    assert_eq!(m.hash(), *blake3::hash(json).as_bytes());
}

#[test]
fn parse_manifest_accepts_chunked_entry_in_order() {
    let m = parse_json(&serde_json::json!({"version": 1, "entries": [{
        "path": "m.bin", "hash": format_hash([0xff; 32]), "size": 100,
        "chunks": [
            {"hash": format_hash([0xc0; 32]), "size": 60},
            {"hash": format_hash([0xc1; 32]), "size": 40},
        ],
    }]}))
    .unwrap();
    let chunks = m.entries[0].chunks.as_ref().expect("chunked entry");
    let hashes: Vec<[u8; 32]> = chunks.iter().map(|c| c.hash).collect();
    assert_eq!(hashes, vec![[0xc0; 32], [0xc1; 32]]);
}

#[test]
fn parse_manifest_plain_entry_has_no_chunks() {
    let m = parse_json(&serde_json::json!({"version": 1, "entries": [
        {"path": "a.txt", "hash": format_hash([0xab; 32]), "size": 4},
    ]}))
    .unwrap();
    assert!(m.entries[0].chunks.is_none());
}

/// A parser takes every spelling `parse_hash` takes: `b3:`, `0x` or no
/// prefix, with upper- or lowercase hex digits.
#[test]
fn parse_manifest_accepts_every_hash_spelling() {
    let hex = blake3::Hash::from_bytes([0xab; 32]).to_hex().to_string();
    let upper = hex.to_uppercase();
    for spelling in [
        format!("b3:{hex}"),
        format!("0x{hex}"),
        hex.clone(),
        format!("b3:{upper}"),
    ] {
        let m = parse_json(&serde_json::json!({"version": 1, "entries": [
            {"path": "a", "hash": spelling},
        ]}))
        .expect(&spelling);
        assert_eq!(m.entries[0].hash, [0xab; 32], "{spelling}");
    }
}

/// One malformed entry or chunk hash makes the whole manifest
/// [`MalformedManifest`], and the error names the entry.
#[test]
fn parse_manifest_rejects_a_malformed_entry_or_chunk_hash() {
    let assert_malformed = |manifest: serde_json::Value, what: &str| {
        let err = parse_json(&manifest).unwrap_err();
        assert!(err.is::<MalformedManifest>(), "{manifest}: {err:#}");
        assert!(format!("{err:#}").contains(what), "{manifest}: {err:#}");
    };
    let good = format_hash([0xab; 32]);
    let hex = "ab".repeat(32);
    for bad in [
        "zz".to_owned(),
        String::new(),
        format!("b3:{}", &hex[..62]),
        format!("b3:{hex}ab"),
        format!("B3:{hex}"),
    ] {
        assert_malformed(
            serde_json::json!({"version": 1, "entries": [
                {"path": "ok", "hash": good},
                {"path": "bad", "hash": bad},
            ]}),
            r#"bundle entry "bad" has a malformed hash"#,
        );
    }
    assert_malformed(
        serde_json::json!({"version": 1, "entries": [{
            "path": "bad", "hash": good, "size": 4,
            "chunks": [{"hash": good, "size": 2}, {"hash": "b3:zz", "size": 2}],
        }]}),
        r#"bundle entry "bad" has a malformed chunk 1 hash"#,
    );
}

/// Two entries whose paths name one file on a case- or
/// normalization-insensitive file system make the whole manifest
/// [`MalformedManifest`], and the error names both paths (#2691).
#[test]
fn parse_manifest_rejects_paths_that_name_one_file() {
    let good = format_hash([0xab; 32]);
    let other = format_hash([0xcd; 32]);
    for (first, second) in [
        ("p", "p"),
        ("README", "readme"),
        ("a/B", "a/b"),
        ("caf\u{e9}", "cafe\u{301}"),
        ("stra\u{df}e", "STRASSE"),
        ("STRA\u{1e9e}E", "strasse"),
        ("\u{390}", "\u{3aa}\u{301}"),
    ] {
        let manifest = serde_json::json!({"version": 1, "entries": [
            {"path": first, "hash": good},
            {"path": "between", "hash": good},
            {"path": second, "hash": other},
        ]});
        let err = parse_json(&manifest).unwrap_err();
        assert!(err.is::<MalformedManifest>(), "{manifest}: {err:#}");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(&format!("{first:?} and {second:?}")),
            "{manifest}: {msg}"
        );
    }
    let distinct = parse_json(&serde_json::json!({"version": 1, "entries": [
        {"path": "a/x", "hash": good},
        {"path": "A/y", "hash": good},
        {"path": "a/xx", "hash": good},
    ]}))
    .expect("paths that name distinct files parse");
    assert_eq!(distinct.entries.len(), 3);
}

/// [`path_collision`] pairs the earliest path whose key repeats with the
/// earlier path it repeats, not the first and last of a set.
#[test]
fn path_collision_names_the_earliest_repeat() {
    assert_eq!(
        super::super::path_collision(["a", "b", "B", "A"]),
        Some(("b", "B"))
    );
    assert_eq!(super::super::path_collision(["a", "b", "c"]), None);
}

/// The version gate runs before any hash parses: a manifest of another
/// version is [`NotAManifest`] for its version,
/// whatever its hashes look like.
#[test]
fn parse_manifest_checks_the_version_before_the_hashes() {
    let json = br#"{"version":2,"entries":[{"path":"a","hash":"sha256:00"}]}"#;
    let err = BundleManifest::parse(json).unwrap_err();
    assert!(err.is::<NotAManifest>(), "{err:#}");
    assert!(
        format!("{err:#}").contains("unsupported bundle version 2"),
        "{err:#}"
    );
}

/// Two entries that spell one blob's hash two ways are one group, so the
/// blob is fetched once.
#[test]
fn group_by_hash_groups_one_blob_across_hash_spellings() {
    let hex = blake3::Hash::from_bytes([0xab; 32]).to_hex().to_string();
    let m = parse_json(&serde_json::json!({"version": 1, "entries": [
        {"path": "a", "hash": format!("b3:{hex}")},
        {"path": "b", "hash": format!("0x{}", hex.to_uppercase())},
    ]}))
    .unwrap();
    let refs: Vec<&ManifestEntry> = m.entries.iter().collect();
    let groups = group_by_hash(&refs);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].entries.len(), 2);
}

#[test]
fn parse_manifest_rejects_unsupported_version() {
    let json = br#"{"version":2,"entries":[]}"#;
    let err = BundleManifest::parse(json).unwrap_err();
    assert!(
        format!("{err:#}").contains("unsupported bundle version 2"),
        "{err:#}"
    );
}

#[test]
fn group_transfer_counts_the_blob_once_and_every_written_copy() {
    // One paid fetch + two linked duplicate paths: downloaded once, three
    // copies reconstructed on disk.
    let outcomes = vec![
        EntryOutcome::Fetched(100),
        EntryOutcome::Linked,
        EntryOutcome::Linked,
    ];
    let t = group_transfer(&outcomes, Some(EntryBytes::whole_blob(100, 0)));
    assert_eq!(t.downloaded, 100);
    assert_eq!(t.reconstructed, 300);
}

/// A range-dedup fetch pays only for the bytes it did not splice from disk,
/// so `downloaded` is its paid tally, not the blob size — while every copy on
/// disk is still a whole file.
#[test]
fn group_transfer_downloads_only_the_paid_bytes_of_a_spliced_blob() {
    let outcomes = vec![EntryOutcome::Fetched(100), EntryOutcome::Linked];
    let spliced = EntryBytes {
        paid: 30,
        spliced: 70,
        resumed: 0,
    };
    let t = group_transfer(&outcomes, Some(spliced));
    assert_eq!(t.downloaded, 30);
    assert_eq!(t.reconstructed, 200);
    assert_eq!(t.resumed, 0);

    // A blob already finalized in staging by an earlier run pays nothing.
    let t = group_transfer(&[EntryOutcome::Fetched(100)], Some(EntryBytes::default()));
    assert_eq!(t.downloaded, 0);
    assert_eq!(t.reconstructed, 100);
}

/// A blob resumed from an earlier run's `.partial` downloads only what this
/// run fetched; the resumed prefix is tallied apart and counted once per
/// blob, however many paths it lands at (#2236).
#[test]
fn group_transfer_tallies_a_resumed_prefix_apart_from_the_download() {
    let outcomes = vec![EntryOutcome::Fetched(100), EntryOutcome::Linked];
    let t = group_transfer(&outcomes, Some(EntryBytes::whole_blob(100, 40)));
    assert_eq!(
        t,
        Transfer {
            downloaded: 60,
            reconstructed: 200,
            resumed: 40,
        }
    );

    // A landed blob whose every destination failed writes nothing, and
    // reports nothing resumed.
    let failed = vec![EntryOutcome::Failed {
        path: "x".into(),
        err: "boom".into(),
    }];
    let t = group_transfer(&failed, Some(EntryBytes::whole_blob(100, 40)));
    assert_eq!(t, Transfer::default());
}

#[test]
fn group_transfer_skips_and_fails_contribute_nothing() {
    let outcomes = vec![EntryOutcome::Fetched(100), EntryOutcome::Skipped];
    let t = group_transfer(&outcomes, None);
    assert_eq!(t.downloaded, 100);
    assert_eq!(t.reconstructed, 100);

    let none = vec![EntryOutcome::Failed {
        path: "x".into(),
        err: "boom".into(),
    }];
    let t = group_transfer(&none, None);
    assert_eq!(t.downloaded, 0);
    assert_eq!(t.reconstructed, 0);
}

/// A pre-pass result that confirms each of `entries` on disk, so each skips.
fn skipping(entries: &[ManifestEntry]) -> DiskState {
    DiskState {
        skip: entries
            .iter()
            .map(|en| (en.path.clone(), en.hash))
            .collect(),
        ..DiskState::default()
    }
}

/// An unchunked, unsized entry at `path` whose hash is `[hash; 32]`.
fn entry(path: &str, hash: u8) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        hash: [hash; 32],
        size: None,
        chunks: None,
        landed_from: None,
    }
}

/// The core of #1306: entries naming the same blob collapse into one group, so
/// the fan-out fetches (and pays for) that blob exactly once. First-seen order
/// is preserved both across groups and within a group.
#[test]
fn group_by_hash_collapses_duplicates_preserving_order() {
    let entries = [
        entry("a.txt", 0x01),
        entry("b.txt", 0x02),
        entry("c.txt", 0x01),
    ];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let paths: Vec<Vec<&str>> = group_by_hash(&refs)
        .iter()
        .map(|group| group.entries.iter().map(|e| e.path.as_str()).collect())
        .collect();
    assert_eq!(paths, vec![vec!["a.txt", "c.txt"], vec!["b.txt"]]);
}

/// Distinct hashes never merge — each is its own unit of work, in order.
#[test]
fn group_by_hash_keeps_distinct_hashes_separate() {
    let entries = [entry("a", 0x01), entry("b", 0x02), entry("c", 0x03)];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let groups = group_by_hash(&refs);
    assert_eq!(groups.len(), 3);
    assert!(groups.iter().all(|group| group.entries.len() == 1));
}

/// Skip-existing / `--overwrite` are decided **per destination** from the
/// [`resolve_disk_state`] pre-pass's skip set (not a raw existence check): one
/// path of a duplicated blob can be in the skip set (Skip) while its twin is
/// not (Write), and `--overwrite` forces both to Write regardless of `skip`.
#[test]
fn plan_slots_classifies_each_destination_independently() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path();
    std::fs::write(out.join("present.txt"), b"x").unwrap();
    let entries = [entry("present.txt", 0x0a), entry("absent.txt", 0x0a)];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let skip = skipping(&entries[..1]);

    let landing = Landing::open(out).unwrap();
    let slots = plan_slots(&refs, &landing, false, &skip);
    assert!(matches!(slots[0], Slot::Skip));
    assert!(matches!(slots[1], Slot::Write { .. }));

    let slots = plan_slots(&refs, &landing, true, &skip);
    assert!(matches!(slots[0], Slot::Write { .. }));
    assert!(matches!(slots[1], Slot::Write { .. }));
}

/// A path confirmed on disk for one blob does not skip an entry at that path
/// with another blob: the skip set is keyed by path and hash (#2691).
#[test]
fn plan_slots_skips_only_the_entry_whose_blob_is_confirmed() {
    let dir = tempfile::tempdir().unwrap();
    let landing = Landing::open(dir.path()).unwrap();
    let skip = skipping(&[entry("p", 0x0a)]);

    let held = [entry("p", 0x0a)];
    let slots = plan_slots(&held.iter().collect::<Vec<_>>(), &landing, false, &skip);
    assert!(matches!(slots[0], Slot::Skip), "{:?}", slots[0]);

    let other = [entry("p", 0x0b)];
    let slots = plan_slots(&other.iter().collect::<Vec<_>>(), &landing, false, &skip);
    assert!(matches!(slots[0], Slot::Write { .. }), "{:?}", slots[0]);
}

/// A manifest built by hand, so it skips the parse check: `{p → A}` and
/// `{p → B}` (or `{P → B}`) with A already at `p`, so a path-keyed skip would
/// pass both. Every colliding entry fails before any fetch, with and without
/// `overwrite`; an entry beside them is unaffected (#2691).
#[tokio::test]
async fn plan_slots_fails_every_entry_whose_path_collides() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path();
    let body = b"blob a";
    std::fs::write(out.join("p"), body).expect("write");
    let a = *blake3::hash(body).as_bytes();
    let landing = Landing::open(out).unwrap();
    for twin in ["p", "P"] {
        let entries = [
            ManifestEntry::new("p", a, None),
            ManifestEntry::new(twin, [0x0b; 32], None),
            entry("q", 0x0c),
        ];
        let refs: Vec<&ManifestEntry> = entries.iter().collect();
        for overwrite in [false, true] {
            let disk =
                resolve_disk_state(&entries, &SavedManifest::default(), out, overwrite).await;
            assert!(disk.skip.is_empty(), "{twin} overwrite={overwrite}");
            let slots = plan_slots(&refs, &landing, overwrite, &disk);
            for (slot, path) in slots.iter().zip(["p", twin]) {
                assert!(
                    matches!(slot, Slot::Failed(EntryOutcome::Failed { path: p, err })
                        if p == path && err.contains("names the same file")),
                    "{twin} overwrite={overwrite}: {slot:?}"
                );
            }
            assert!(matches!(slots[2], Slot::Write { .. }), "{:?}", slots[2]);
        }
    }
}

/// Two entries that [`ManifestEntry::land_at`] moves to one path collide on
/// the landed path, and both fail.
#[tokio::test]
async fn plan_slots_fails_entries_landed_at_one_path() {
    let tmp = tempfile::tempdir().expect("tmp");
    let entries = [
        entry("a/config.json", 0x0a).land_at("config.json"),
        entry("b/config.json", 0x0b).land_at("config.json"),
    ];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let disk = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    let slots = plan_slots(&refs, &Landing::open(tmp.path()).unwrap(), false, &disk);
    assert!(
        slots.iter().all(|slot| matches!(slot, Slot::Failed(_))),
        "{slots:?}"
    );
}

/// A path that escapes `out_root` is a per-destination failure, not a fetch.
#[test]
fn plan_slots_marks_unsafe_paths_failed() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [entry("../escape", 0x0a)];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let slots = plan_slots(
        &refs,
        &Landing::open(dir.path()).unwrap(),
        false,
        &DiskState::default(),
    );
    assert!(matches!(slots[0], Slot::Failed(_)));
}

/// A manifest path inside the reserved staging dir must not be materialized —
/// otherwise it could collide with a per-hash staging file and `remove_staging`
/// could delete a real output.
#[test]
fn plan_slots_rejects_the_reserved_staging_dir() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [entry(&format!("{STAGING_DIR}/deadbeef.partial"), 0x0a)];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let slots = plan_slots(
        &refs,
        &Landing::open(dir.path()).unwrap(),
        false,
        &DiskState::default(),
    );
    assert!(matches!(slots[0], Slot::Failed(_)));
}

/// A manifest path at the skip-cache file or inside the manifest cache must not
/// be materialized: the run's own state writes would replace or collide with it.
#[test]
fn plan_slots_rejects_the_pull_state_paths() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [
        entry(skip_cache::SAVED_MANIFEST_NAME, 0x0a),
        entry(&format!("{}/x.json", manifest_cache::CACHE_DIR), 0x0a),
        entry("plain.bin", 0x0a),
    ];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let slots = plan_slots(
        &refs,
        &Landing::open(dir.path()).unwrap(),
        false,
        &DiskState::default(),
    );
    assert!(matches!(slots[0], Slot::Failed(_)));
    assert!(matches!(slots[1], Slot::Failed(_)));
    assert!(matches!(slots[2], Slot::Write { .. }));
}

/// An entry whose parent chain leads out of the root fails before its fetch
/// is paid for; its sibling is still written.
#[cfg(unix)]
#[test]
fn plan_slots_fails_an_entry_whose_parent_leads_out_of_the_root() {
    let out = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), out.path().join("link")).unwrap();
    let landing = Landing::open(out.path()).unwrap();
    let entries = [entry("link/escape.bin", 0x0a), entry("safe.bin", 0x0a)];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();

    let slots = plan_slots(&refs, &landing, false, &DiskState::default());

    assert!(
        matches!(&slots[0], Slot::Failed(EntryOutcome::Failed { path, err })
            if path == "link/escape.bin" && err.contains("outside the output directory")),
        "{:?}",
        slots[0]
    );
    assert!(matches!(slots[1], Slot::Write { .. }));
}

/// An entry already present behind a link the handle refuses is skipped,
/// not failed: nothing is written to it. With `--overwrite` it is written,
/// so the check applies and fails it before the fetch.
#[cfg(unix)]
#[test]
fn plan_slots_skips_a_present_entry_before_it_checks_its_parent_chain() {
    let out = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), out.path().join("link")).unwrap();
    let landing = Landing::open(out.path()).unwrap();
    let entries = [entry("link/file", 0x0a)];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let skip = skipping(&entries);

    let slots = plan_slots(&refs, &landing, false, &skip);
    assert!(matches!(slots[0], Slot::Skip), "{:?}", slots[0]);

    let slots = plan_slots(&refs, &landing, true, &DiskState::default());
    assert!(matches!(slots[0], Slot::Failed(_)), "{:?}", slots[0]);
}

/// A run stopped before any group started has one `NotRun` outcome per
/// entry, so the summary and the pass merge see every entry.
#[test]
fn a_run_stopped_before_start_has_one_not_run_outcome_per_entry() {
    let run = PullRun::stopped_before_start(3, anyhow!("the output directory cannot be opened"));
    assert_eq!(run.outcomes.len(), 3);
    assert!(
        run.outcomes
            .iter()
            .all(|o| matches!(o, EntryOutcome::NotRun))
    );
    assert!(!run.interrupted);
    assert!(run.stopped.is_some());
}

/// The landing gate holds for a slot the plan let through: the escaping
/// destination fails, the next path of the group lands from the same staging
/// blob, and nothing reaches the directory the link points at.
#[cfg(unix)]
#[tokio::test]
async fn a_group_whose_path_crosses_a_symlink_out_of_the_root_fails_that_entry_only() {
    let out = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), out.path().join("link")).unwrap();
    let landing = Arc::new(Landing::open(out.path()).unwrap());
    let staging = out.path().join(STAGING_DIR).join("blob");
    std::fs::create_dir_all(staging.parent().unwrap()).unwrap();
    std::fs::write(&staging, b"bytes").unwrap();
    let slots = vec![
        Slot::Write {
            label: "link/escape.bin",
            dest: out.path().join("link/escape.bin"),
        },
        Slot::Write {
            label: "safe.bin",
            dest: out.path().join("safe.bin"),
        },
    ];

    let outcomes = materialize_group(
        slots,
        |dest| {
            let landing = landing.clone();
            let staging = staging.clone();
            off_runtime("materialize", move || {
                landing.first_write(&staging, &dest, false)
            })
        },
        |src, dest| link_off_runtime(landing.clone(), src, dest),
    )
    .await;

    assert!(
        matches!(&outcomes[0], EntryOutcome::Failed { path, err }
            if path == "link/escape.bin" && err.contains("outside the output directory")),
        "{:?}",
        outcomes[0]
    );
    assert!(
        matches!(outcomes[1], EntryOutcome::Fetched(5)),
        "{:?}",
        outcomes[1]
    );
    assert_eq!(
        std::fs::read(out.path().join("safe.bin")).unwrap(),
        b"bytes"
    );
    assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
}

/// Record `entries` as landed under `out` from the bundle `bundle`, as a pull
/// does once each one lands.
fn record_landed(out: &std::path::Path, bundle: [u8; 32], entries: &[&ManifestEntry]) {
    let outcomes = vec![EntryOutcome::Skipped; entries.len()];
    let updates = build_completed_updates(entries, &outcomes, bundle, out);
    let mut saved = skip_cache::load(out);
    skip_cache::merge(&mut saved, updates);
    skip_cache::write_bytes(out, &skip_cache::serialize(&saved).unwrap()).unwrap();
}

/// `foreign_paths` names each entry path that no record vouches for: one no
/// pull recorded, one that changed after a pull wrote it, and one a pull wrote
/// from another bundle entry with other bytes. It passes an absent path, a
/// file this entry wrote, a file another entry wrote with the same bytes, and
/// a path the pull reserves for its own state.
#[test]
fn foreign_paths_names_each_path_no_record_vouches_for() {
    use crate::bundle::ForeignReason::{Changed, OtherSource, Unrecorded};
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path();
    let (this, other) = ([1u8; 32], [2u8; 32]);
    for name in [
        "mine.txt",
        "pulled.bin",
        "renamed.json",
        "shared.txt",
        "same.txt",
        "edited.txt",
    ] {
        std::fs::write(out.join(name), b"abc").unwrap();
    }
    std::fs::create_dir(out.join("sub")).unwrap();
    let pulled = entry("pulled.bin", 0x0a);
    let landed = entry("a/config.json", 0x0a).land_at("renamed.json");
    let edited = entry("edited.txt", 0x0a);
    record_landed(out, this, &[&pulled, &landed, &edited]);
    record_landed(
        out,
        other,
        &[&entry("shared.txt", 0x0a), &entry("same.txt", 0x0e)],
    );
    std::fs::write(out.join("edited.txt"), b"abcd").unwrap();

    let manifest = BundleManifest::new(
        this,
        vec![
            entry("mine.txt", 0x0a),
            entry("sub", 0x0a),
            pulled,
            landed,
            entry("b/config.json", 0x0b).land_at("renamed.json"),
            entry("shared.txt", 0x0d),
            entry("same.txt", 0x0e),
            edited,
            entry(".decdn-manifest.json", 0x0a),
            entry("absent.bin", 0x0a),
        ],
    );
    let from = |hash, path: &str| {
        Some(crate::uri::DecdnUri::new(
            hash,
            crate::uri::UriTarget::BundleEntry(path.to_owned()),
            None,
        ))
    };
    let got: Vec<_> = crate::bundle::foreign_paths(&manifest, out)
        .into_iter()
        .map(|f| (f.path, f.reason))
        .collect();
    assert_eq!(
        got,
        [
            ("mine.txt".to_owned(), Unrecorded),
            ("sub".to_owned(), Unrecorded),
            (
                "renamed.json".to_owned(),
                OtherSource(from(this, "a/config.json"))
            ),
            (
                "shared.txt".to_owned(),
                OtherSource(from(other, "shared.txt"))
            ),
            ("edited.txt".to_owned(), Changed(from(this, "edited.txt"))),
        ]
    );
}

/// A skip-cache that is there but cannot be used vouches for no path, and
/// says why; a record whose manifest hash cannot be read still refuses, with
/// no source to name.
#[test]
fn foreign_paths_reports_an_unusable_cache_and_an_unreadable_source() {
    use crate::bundle::ForeignReason::{OtherSource, UnreadableCache};
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path();
    std::fs::write(out.join("a.txt"), b"abc").unwrap();
    let manifest = BundleManifest::new([1; 32], vec![entry("a.txt", 0x0a)]);

    std::fs::write(
        out.join(".decdn-manifest.json"),
        br#"{"version":1,"files":{}}"#,
    )
    .unwrap();
    let found = crate::bundle::foreign_paths(&manifest, out);
    let [only] = found.as_slice() else {
        panic!("one foreign path: {found:?}");
    };
    let UnreadableCache(cause) = &only.reason else {
        panic!("an unusable cache: {only:?}");
    };
    assert!(cause.contains("version 1"), "{cause}");

    let mut rec = record_one(out, [1; 32], &entry("a.txt", 0x0c)).unwrap();
    rec.source.bundle = "not-a-hash".to_owned();
    let mut saved = SavedManifest::default();
    skip_cache::merge(&mut saved, BTreeMap::from([("a.txt".to_owned(), rec)]));
    skip_cache::write_bytes(out, &skip_cache::serialize(&saved).unwrap()).unwrap();
    assert_eq!(
        crate::bundle::foreign_paths(&manifest, out)
            .into_iter()
            .map(|f| f.reason)
            .collect::<Vec<_>>(),
        [OtherSource(None)]
    );
}

/// A second `land_at` keeps the first manifest path.
#[test]
fn land_at_twice_keeps_the_first_manifest_path() {
    let en = entry("a/x", 0x0a).land_at("x").land_at("y");
    assert_eq!(en.path, "y");
    assert_eq!(en.manifest_path(), "a/x");
}

/// A record names the entry's manifest path, not the name `land_at` gave it,
/// and the manifest's hash as its bundle.
#[test]
fn record_one_names_the_manifest_path_of_a_landed_entry() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("config.json"), b"{}").unwrap();
    let en = entry("text_encoder/config.json", 0x0a).land_at("config.json");
    assert_eq!(en.path, "config.json");
    assert_eq!(en.manifest_path(), "text_encoder/config.json");
    let rec = record_one(tmp.path(), [7; 32], &en).expect("record");
    assert_eq!(
        rec.source,
        skip_cache::SavedSource::new([7; 32], "text_encoder/config.json")
    );
    assert!(rec.source.is([7; 32], "text_encoder/config.json"));
}

/// An unrecorded (empty saved manifest) file still skips via the re-hash gate
/// when its on-disk bytes already match the new manifest hash.
#[tokio::test]
async fn resolve_disk_state_skips_matching_file_by_rehash() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = b"hello world";
    std::fs::write(tmp.path().join("a.txt"), body).expect("write");
    let h = *blake3::hash(body).as_bytes();
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: h,
        size: Some(u64::try_from(body.len()).expect("len")),
        chunks: None,
        landed_from: None,
    }];
    // Empty saved manifest → falls to the re-hash gate, still skips (content matches).
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(st.skip.contains_key("a.txt"));
}

/// The fast path skips WITHOUT re-hashing when a saved record's hash, size,
/// and mtime all match the on-disk file — the file's bytes are never read.
/// Proven by planting bytes that do NOT hash to the recorded hash: a re-hash
/// would mismatch and fetch, so a skip can only mean the record was trusted.
#[tokio::test]
async fn resolve_disk_state_fast_skips_on_matching_record_without_rehash() {
    let tmp = tempfile::tempdir().expect("tmp");
    // On-disk bytes deliberately do not hash to the claimed hash below.
    let body = b"actual on-disk bytes";
    std::fs::write(tmp.path().join("a.txt"), body).expect("write");
    let meta = std::fs::metadata(tmp.path().join("a.txt")).expect("meta");
    let size = u64::try_from(body.len()).expect("len");
    let mtime = SavedMtime::of(&meta).expect("mtime");
    // A hash the on-disk bytes provably do not produce.
    let claimed = [0x00; 32];
    // Document the assumption the skip proof rests on: the planted bytes do
    // not hash to `claimed`, so a re-hash would fetch and only a
    // record-trusting fast-skip can pass.
    assert_ne!(
        claimed,
        *blake3::hash(body).as_bytes(),
        "planted bytes must not match the claimed hash"
    );
    let mut updates = BTreeMap::new();
    updates.insert(
        "a.txt".to_string(),
        skip_cache::SavedFile {
            hash: format_hash(claimed),
            size,
            mtime,
            chunks: None,
            source: test_source(),
        },
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = skip_cache::load(tmp.path());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: claimed, // equals the saved record's hash → fast-skip candidate
        size: Some(size),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &saved, tmp.path(), false).await;
    // Skipped on the record alone; a re-hash of the (non-matching) bytes would fetch.
    assert!(st.skip.contains_key("a.txt"));
}

/// An entry whose blob landed at a size other than its manifest's is
/// accepted on the next run by its record's hash: the manifest size is only
/// a first claim, so it is neither fetched nor re-hashed again.
#[tokio::test]
async fn a_mismatched_entry_is_not_refetched_on_rerun() {
    let tmp = tempfile::tempdir().expect("tmp");
    // Bytes that do not hash to the recorded hash: only a record-trusting
    // skip can pass, so a skip proves no re-hash ran.
    let body = b"the blob is longer than the manifest says";
    std::fs::write(tmp.path().join("a.bin"), body).expect("write");
    let meta = std::fs::metadata(tmp.path().join("a.bin")).expect("meta");
    let size = u64::try_from(body.len()).expect("len");
    let hash = [0x11; 32];
    let mut updates = BTreeMap::new();
    updates.insert(
        "a.bin".to_string(),
        skip_cache::SavedFile {
            hash: format_hash(hash),
            size,
            mtime: SavedMtime::of(&meta).expect("mtime"),
            chunks: None,
            source: test_source(),
        },
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = skip_cache::load(tmp.path());
    let entries = [ManifestEntry {
        path: "a.bin".into(),
        hash,
        size: Some(size - 7),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &saved, tmp.path(), false).await;
    assert!(st.skip.contains_key("a.bin"), "accepted by its hash");
}

/// A file whose mtime drifted but whose content is unchanged is NOT
/// re-fetched: the fast path misses (recorded mtime differs), and the re-hash
/// gate then confirms the on-disk bytes against the new manifest hash and
/// skips. This is the "touched but identical" case.
#[tokio::test]
async fn resolve_disk_state_rehash_confirms_touched_but_identical_file() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = b"unchanged content";
    std::fs::write(tmp.path().join("a.txt"), body).expect("write");
    let real = *blake3::hash(body).as_bytes();
    let size = u64::try_from(body.len()).expect("len");
    // Right hash + size, but a stale (1970) mtime → the fast path misses.
    let stale = SavedMtime { secs: 1, nanos: 0 };
    // Document the precondition: the stale mtime differs from the file's
    // actual mtime, so the fast path is genuinely bypassed and the skip can
    // only come from the re-hash gate.
    let meta = std::fs::metadata(tmp.path().join("a.txt")).expect("meta");
    assert_ne!(
        stale,
        SavedMtime::of(&meta).expect("mtime"),
        "stale mtime must differ from the file's real mtime"
    );
    let mut updates = BTreeMap::new();
    updates.insert(
        "a.txt".to_string(),
        skip_cache::SavedFile {
            hash: format_hash(real),
            size,
            mtime: stale,
            chunks: None,
            source: test_source(),
        },
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = skip_cache::load(tmp.path());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: real,
        size: Some(size),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &saved, tmp.path(), false).await;
    // Re-hash confirmed the content is identical → skip, no re-fetch.
    assert!(st.skip.contains_key("a.txt"));
}

/// A changed file (content no longer matches the new manifest hash) is never
/// skipped, whether or not a saved record exists for it.
#[tokio::test]
async fn resolve_disk_state_fetches_changed_file() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("a.txt"), b"OLD CONTENT").expect("write");
    let new = *blake3::hash(b"NEW CONTENT").as_bytes();
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: new,
        size: Some(11),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains_key("a.txt")); // hash mismatch → fetch
}

/// An absent file always fetches — nothing to re-hash, no fast-skip possible.
#[tokio::test]
async fn resolve_disk_state_absent_file_fetches() {
    let tmp = tempfile::tempdir().expect("tmp");
    let entries = [ManifestEntry {
        path: "missing.txt".into(),
        hash: [0x00; 32],
        size: Some(1),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains_key("missing.txt"));
}

/// `--overwrite` bypasses the pre-pass entirely: nothing is ever skipped.
#[tokio::test]
async fn resolve_disk_state_overwrite_skips_nothing() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = b"hello world";
    std::fs::write(tmp.path().join("a.txt"), body).expect("write");
    let h = *blake3::hash(body).as_bytes();
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: h,
        size: Some(u64::try_from(body.len()).expect("len")),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), true).await;
    assert!(st.skip.is_empty());
}

/// A saved record for a file that still exists on disk becomes a whole-file
/// donor keyed by its hash — even when its path is NOT in the current bundle
/// (the cross-bundle / shared-file case).
#[tokio::test]
async fn resolve_disk_state_indexes_whole_file_donor_from_other_path() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = vec![7u8; 4096];
    std::fs::create_dir_all(tmp.path().join("game1/lib")).expect("mkdir");
    std::fs::write(tmp.path().join("game1/lib/dup.dll"), &body).expect("write");
    let h = *blake3::hash(&body).as_bytes();
    // Prior run recorded game1/lib/dup.dll.
    let old_entries = [ManifestEntry {
        path: "game1/lib/dup.dll".into(),
        hash: h,
        size: Some(4096),
        chunks: None,
        landed_from: None,
    }];
    let updates = build_completed_updates(
        &old_entries.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(4096)],
        TEST_BUNDLE,
        tmp.path(),
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved");
    let saved = skip_cache::load(tmp.path());
    // The NEW bundle wants the same content at a different path.
    let new_entries = vec![ManifestEntry {
        path: "game2/lib/dup.dll".into(),
        hash: h,
        size: Some(4096),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&new_entries, &saved, tmp.path(), false).await;
    assert_eq!(
        st.whole_file.get(&h),
        Some(&tmp.path().join("game1/lib/dup.dll"))
    );
}

/// A saved record whose path IS an in-scope current-bundle entry that will be
/// WRITTEN this run (its content changed, so it is not in `state.skip`) is
/// excluded from `whole_file`: another entry's group could atomically replace
/// its bytes between the whole-file link's re-hash and its link/copy (TOCTOU),
/// so it must never be indexed as a whole-file donor — even though its bytes
/// are still a valid CHUNK donor (covered by the final whole-file
/// re-verification) and stay seeded.
#[tokio::test]
async fn resolve_disk_state_excludes_will_write_path_from_whole_file_index() {
    let tmp = tempfile::tempdir().expect("tmp");
    let old_body = vec![9u8; 32];
    std::fs::write(tmp.path().join("changed.bin"), &old_body).expect("write old");
    let old_hash = *blake3::hash(&old_body).as_bytes();
    let old_entries = [ManifestEntry {
        path: "changed.bin".into(),
        hash: old_hash,
        size: Some(32),
        chunks: None,
        landed_from: None,
    }];
    let updates = build_completed_updates(
        &old_entries.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(32)],
        TEST_BUNDLE,
        tmp.path(),
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved");
    let saved = skip_cache::load(tmp.path());

    // The new manifest wants DIFFERENT content at the SAME path: this entry
    // is in scope and will be written (not skipped), so its saved record's
    // hash must not become a whole-file donor.
    let new_hash = *blake3::hash(&[1u8; 32]).as_bytes();
    let new_entries = vec![ManifestEntry {
        path: "changed.bin".into(),
        hash: new_hash,
        size: Some(32),
        chunks: None,
        landed_from: None,
    }];

    let st = resolve_disk_state(&new_entries, &saved, tmp.path(), false).await;
    assert!(
        !st.skip.contains_key("changed.bin"),
        "changed content must fetch"
    );
    assert_eq!(
        st.whole_file.get(&old_hash),
        None,
        "a will-write path must never be indexed as a whole-file donor"
    );
}

/// A saved record at `readme` is no whole-file donor while this run writes
/// `README`: on a case-insensitive file system the two are one file, so the
/// write could replace the donor's bytes between its re-hash and its link.
#[tokio::test]
async fn resolve_disk_state_excludes_a_case_variant_of_a_will_write_path() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor_body = vec![9u8; 32];
    std::fs::write(tmp.path().join("readme"), &donor_body).expect("write donor");
    let donor_hash = *blake3::hash(&donor_body).as_bytes();
    let old_entries = [ManifestEntry::new("readme", donor_hash, Some(32))];
    let updates = build_completed_updates(
        &old_entries.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(32)],
        TEST_BUNDLE,
        tmp.path(),
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved");
    let saved = skip_cache::load(tmp.path());

    let written = ManifestEntry::new("README", *blake3::hash(&[1u8; 32]).as_bytes(), Some(32));
    let copy = ManifestEntry::new("copy.bin", donor_hash, Some(32));
    let st = resolve_disk_state(&[written, copy.clone()], &saved, tmp.path(), false).await;
    assert_eq!(st.whole_file.get(&donor_hash), None);

    // Without the case-variant write, the same record is a donor.
    let st = resolve_disk_state(&[copy], &saved, tmp.path(), false).await;
    assert_eq!(
        st.whole_file.get(&donor_hash),
        Some(&tmp.path().join("readme"))
    );
}

/// `overwrite` builds no index (nothing is reused).
#[tokio::test]
async fn resolve_disk_state_overwrite_builds_no_whole_file_index() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = vec![7u8; 16];
    std::fs::write(tmp.path().join("a.bin"), &body).expect("write");
    let h = *blake3::hash(&body).as_bytes();
    let old = [ManifestEntry {
        path: "a.bin".into(),
        hash: h,
        size: Some(16),
        chunks: None,
        landed_from: None,
    }];
    let updates = build_completed_updates(
        &old.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(16)],
        TEST_BUNDLE,
        tmp.path(),
    );
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates).expect("w");
    let saved = skip_cache::load(tmp.path());
    let st = resolve_disk_state(&old, &saved, tmp.path(), true).await;
    assert!(st.whole_file.is_empty());
}

/// A symlink at a destination path is never fast-skipped, even when it
/// points at content whose bytes match the manifest hash: the pre-pass uses
/// `symlink_metadata` and requires a regular file, so the symlink is left to
/// the fetch path (which materializes a regular file over it) rather than
/// hashing the link target or keeping the link in place.
#[cfg(unix)]
#[tokio::test]
async fn resolve_disk_state_does_not_skip_a_symlink() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = b"hello world";
    // The real bytes live outside the manifest path; the manifest path is a
    // symlink to them, so following it would hash a match.
    std::fs::write(tmp.path().join("target.bin"), body).expect("write target");
    std::os::unix::fs::symlink(tmp.path().join("target.bin"), tmp.path().join("link.txt"))
        .expect("symlink");
    let h = *blake3::hash(body).as_bytes();
    let entries = [ManifestEntry {
        path: "link.txt".into(),
        hash: h,
        size: Some(u64::try_from(body.len()).expect("len")),
        chunks: None,
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains_key("link.txt"));
    assert!(st.seed.is_empty());
}

/// A skipped (unchanged) path seeds the NEW manifest entry's chunks, sourced
/// from its own output file — a donor future entries can splice from without
/// paying, spanning cross-run and cross-bundle reuse.
#[tokio::test]
async fn resolve_disk_state_seeds_unchanged_and_old_chunks() {
    let tmp = tempfile::tempdir().expect("tmp");
    // unchanged file present, matches new manifest, has chunks → seed NEW chunks
    let body = vec![9u8; 20];
    std::fs::write(tmp.path().join("u.bin"), &body).expect("write");
    let uh = *blake3::hash(&body).as_bytes();
    let ch = *blake3::hash(&body).as_bytes(); // single-chunk == whole file
    let entries = [ManifestEntry {
        path: "u.bin".into(),
        hash: uh,
        size: Some(20),
        chunks: Some(vec![ManifestChunk { hash: ch, size: 20 }]),
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(st.skip.contains_key("u.bin"));
    assert_eq!(st.seed.len(), 1);
    assert_eq!(st.seed[0].offset, 0);
    assert_eq!(st.seed[0].len, 20);
}

/// A changed path (present but hash-mismatched against the new manifest)
/// with a prior saved record carrying chunks seeds the OLD chunks, sourced
/// from the still-present old file — it survives on disk until this entry's
/// own group atomically materializes.
#[tokio::test]
async fn resolve_disk_state_seeds_old_chunks_of_a_changed_file() {
    let tmp = tempfile::tempdir().expect("tmp");
    let old_body = vec![1u8; 20];
    let path = tmp.path().join("c.bin");
    std::fs::write(&path, &old_body).expect("write old");

    // Build a saved record (as a prior run would have) carrying chunk hints
    // for the OLD content, then persist and reload it via the real
    // merge_and_write / load round trip.
    let old_hash = *blake3::hash(&old_body).as_bytes();
    let old_chunk_hash = old_hash; // single-chunk == whole file
    let old_entries = [ManifestEntry {
        path: "c.bin".into(),
        hash: old_hash,
        size: Some(20),
        chunks: Some(vec![ManifestChunk {
            hash: old_chunk_hash,
            size: 20,
        }]),
        landed_from: None,
    }];
    let old_outcomes = vec![EntryOutcome::Fetched(20)];
    let old_refs: Vec<&ManifestEntry> = old_entries.iter().collect();
    let updates = build_completed_updates(&old_refs, &old_outcomes, TEST_BUNDLE, tmp.path());
    skip_cache::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = skip_cache::load(tmp.path());

    // The new manifest declares different content, but the OLD file is
    // still on disk (not yet overwritten) — a fetch, not a skip, and the
    // old bytes remain a valid donor until this entry's own materialize.
    let new_hash = *blake3::hash(&[2u8; 20]).as_bytes();
    let new_entries = vec![ManifestEntry {
        path: "c.bin".into(),
        hash: new_hash,
        size: Some(20),
        chunks: None,
        landed_from: None,
    }];

    let st = resolve_disk_state(&new_entries, &saved, tmp.path(), false).await;
    assert!(!st.skip.contains_key("c.bin"), "changed content must fetch");
    // The per-entry "changed" branch and the whole-root records pass both
    // seed this same donor (harmless duplication, see resolve_disk_state's
    // doc comment) — assert every seed entry present matches, rather than
    // pinning an exact count.
    assert!(!st.seed.is_empty());
    for donor in &st.seed {
        assert_eq!(donor.hash, old_chunk_hash);
        assert_eq!(donor.source, path);
        assert_eq!(donor.offset, 0);
        assert_eq!(donor.len, 20);
    }
}

/// The headline #1306 invariant, testable without a live endpoint: two
/// destinations for one blob → the paid `materialize` runs **once**, the second
/// is a free `link`, and outcomes are `[Fetched, Linked]` (never `Fetched`
/// twice, which would imply a double payment).
#[tokio::test]
async fn materialize_group_fetches_once_and_links_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let slots = vec![
        Slot::Write {
            label: "a",
            dest: dir.path().join("a"),
        },
        Slot::Write {
            label: "b",
            dest: dir.path().join("b"),
        },
    ];
    let mat_calls = std::cell::Cell::new(0u32);
    let link_calls = std::cell::Cell::new(0u32);

    let outcomes = materialize_group(
        slots,
        |_dest| {
            mat_calls.set(mat_calls.get() + 1);
            async { Ok(7u64) }
        },
        |_src, _dest| {
            link_calls.set(link_calls.get() + 1);
            async { Ok(()) }
        },
    )
    .await;

    assert_eq!(mat_calls.get(), 1, "the paid path runs exactly once");
    assert_eq!(
        link_calls.get(),
        1,
        "the duplicate is linked, not re-fetched"
    );
    assert!(matches!(outcomes[0], EntryOutcome::Fetched(7)));
    assert!(matches!(outcomes[1], EntryOutcome::Linked));
}

/// If the first writable destination fails to materialize, the next one retries
/// from the same in-memory bytes (no re-fetch), so one bad path can't doom the
/// group and nothing is linked from a non-existent canonical.
#[tokio::test]
async fn materialize_group_retries_next_path_when_first_write_fails() {
    let dir = tempfile::tempdir().unwrap();
    let slots = vec![
        Slot::Write {
            label: "a",
            dest: dir.path().join("a"),
        },
        Slot::Write {
            label: "b",
            dest: dir.path().join("b"),
        },
    ];
    let mat_calls = std::cell::Cell::new(0u32);
    let link_calls = std::cell::Cell::new(0u32);

    let outcomes = materialize_group(
        slots,
        |_dest| {
            let n = mat_calls.get();
            mat_calls.set(n + 1);
            async move {
                if n == 0 {
                    Err(anyhow!("first write failed"))
                } else {
                    Ok(5u64)
                }
            }
        },
        |_src, _dest| {
            link_calls.set(link_calls.get() + 1);
            async { Ok(()) }
        },
    )
    .await;

    assert_eq!(
        mat_calls.get(),
        2,
        "the second writable path retries materialize"
    );
    assert_eq!(
        link_calls.get(),
        0,
        "no canonical to link from until one succeeds"
    );
    assert!(matches!(outcomes[0], EntryOutcome::Failed { .. }));
    assert!(matches!(outcomes[1], EntryOutcome::Fetched(5)));
}

/// `materialize_from_donor` links every write slot from an on-disk donor —
/// no fetch, no payment — and tags the reused blob `Deduped`, mirroring
/// `materialize_group`'s fetch-once/link-rest accounting.
#[tokio::test]
async fn materialize_from_donor_links_every_destination() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor = tmp.path().join("game1/lib/dup.dll");
    std::fs::create_dir_all(donor.parent().expect("parent")).expect("mkdir");
    let body = vec![3u8; 2048];
    std::fs::write(&donor, &body).expect("write donor");
    let dest = tmp.path().join("game2/lib/dup.dll");
    let slots = vec![Slot::Write {
        label: "game2/lib/dup.dll",
        dest: dest.clone(),
    }];
    let landing = Arc::new(Landing::open(tmp.path()).unwrap());
    let outcomes = materialize_from_donor(&landing, slots, &donor, 2048).await;
    assert!(matches!(outcomes.as_slice(), [EntryOutcome::Deduped(2048)]));
    assert_eq!(std::fs::read(&dest).expect("read"), body);
}

/// A second duplicate destination is a free `Linked`, not a second
/// `Deduped` — the whole point of routing this through `materialize_group`.
#[tokio::test]
async fn materialize_from_donor_links_second_destination_as_linked() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor = tmp.path().join("donor.bin");
    std::fs::write(&donor, b"same-bytes").expect("write donor");
    let dest_a = tmp.path().join("a/out.bin");
    let dest_b = tmp.path().join("b/out.bin");
    let slots = vec![
        Slot::Write {
            label: "a/out.bin",
            dest: dest_a.clone(),
        },
        Slot::Write {
            label: "b/out.bin",
            dest: dest_b.clone(),
        },
    ];
    let landing = Arc::new(Landing::open(tmp.path()).unwrap());
    let outcomes = materialize_from_donor(&landing, slots, &donor, 10).await;
    assert!(matches!(outcomes[0], EntryOutcome::Deduped(10)));
    assert!(matches!(outcomes[1], EntryOutcome::Linked));
    assert_eq!(std::fs::read(&dest_a).expect("read a"), b"same-bytes");
    assert_eq!(std::fs::read(&dest_b).expect("read b"), b"same-bytes");
}

/// A `Slot::Failed` slot passes through as its own failure, never touched
/// by the donor materialize path.
#[tokio::test]
async fn materialize_from_donor_preserves_failed_slot() {
    let tmp = tempfile::tempdir().expect("tmp");
    let donor = tmp.path().join("donor.bin");
    std::fs::write(&donor, b"bytes").expect("write donor");
    let dest = tmp.path().join("out.bin");
    let slots = vec![
        Slot::Failed(EntryOutcome::failed("bad", &anyhow!("resolve failed"))),
        Slot::Write {
            label: "out.bin",
            dest: dest.clone(),
        },
    ];
    let landing = Arc::new(Landing::open(tmp.path()).unwrap());
    let outcomes = materialize_from_donor(&landing, slots, &donor, 5).await;
    assert!(matches!(outcomes[0], EntryOutcome::Failed { .. }));
    assert!(matches!(outcomes[1], EntryOutcome::Deduped(5)));
}

/// Cache `manifest` under `out` as a bundle `decdn get` caches it, and return
/// the bundle's `b3:` hash.
fn cache_manifest(out: &Path, manifest: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(manifest).unwrap();
    let hash = *blake3::hash(&bytes).as_bytes();
    manifest_cache::store(out, hash, &bytes);
    format!("b3:{}", blake3::Hash::from_bytes(hash).to_hex())
}

/// A real pull reads a cached manifest instead of fetching it, and an
/// overwriting pull skips the cache to force a fresh fetch.
#[test]
fn cached_manifest_reads_the_cache_unless_overwrite() {
    let tmp = tempfile::tempdir().expect("tmp");
    let manifest = serde_json::json!({"version": 1, "entries": []});
    let hash = cache_manifest(tmp.path(), &manifest);
    let raw = parse_hash(&hash).expect("hash");

    let cached = cached_manifest(tmp.path(), raw, true, |w| panic!("warned: {w}"));
    assert_eq!(cached, Some(serde_json::to_vec(&manifest).unwrap()));

    assert_eq!(cached_manifest(tmp.path(), raw, false, |_| {}), None);
}

/// A real pull is fail-open on a cache file it cannot read: it fetches the
/// manifest instead of failing (#2361).
#[test]
fn cached_manifest_falls_back_to_a_fetch_on_an_unreadable_cache_file() {
    let tmp = tempfile::tempdir().expect("tmp");
    let raw = [0xcd_u8; 32];
    std::fs::create_dir_all(manifest_cache::cache_path(tmp.path(), raw)).expect("mkdir");

    let mut warned = None;
    assert_eq!(
        cached_manifest(tmp.path(), raw, true, |w| warned = Some(w.to_string())),
        None
    );
    assert!(
        warned.is_some_and(|w| w.ends_with("; fetching the bundle manifest")),
        "the unreadable cache is reported"
    );
}

/// A candidate on node `node` run by operator `operator`.
fn stripe_candidate(node: u8, operator: u8) -> NodeCandidate {
    NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[node; 32]).public(),
        eth_address: Address::from([operator; 20]),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    }
}

/// Settling a group records its outcomes, flushes only recordable files,
/// and hands back the fault that ends the pull, if any.
#[test]
fn settle_group_run_records_outcomes_and_passes_a_stop_through() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut groups = vec![SettledGroup::default(), SettledGroup::default()];
    let slot = || {
        vec![Slot::Write {
            label: "a.bin",
            dest: PathBuf::from("a.bin"),
        }]
    };
    let failed = GroupRun::fetch_failed(slot(), anyhow!("stream reset"));
    assert!(settle_group_run(&mut groups, &tx, 0, failed, BTreeMap::new(), Vec::new()).is_none());
    assert!(matches!(
        groups[0].outcomes.as_slice(),
        [EntryOutcome::Failed { .. }]
    ));
    assert_eq!(groups[0].bytes, None, "a failed fetch paid for nothing");
    assert!(rx.try_recv().is_err(), "nothing recorded, nothing flushed");

    let disk = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::StorageFull))
        .context("write staging");
    let stopped = settle_group_run(
        &mut groups,
        &tx,
        0,
        GroupRun::fetch_failed(slot(), disk),
        BTreeMap::new(),
        Vec::new(),
    );
    assert!(stopped.is_some(), "a local disk fault ends the pull");

    let mut updates = BTreeMap::new();
    updates.insert(
        "a.bin".to_string(),
        skip_cache::SavedFile {
            hash: format_hash([0x00; 32]),
            size: 3,
            mtime: SavedMtime { secs: 1, nanos: 0 },
            chunks: None,
            source: test_source(),
        },
    );
    let landed = GroupRun::landed(vec![EntryOutcome::Fetched(3)], EntryBytes::whole_blob(3, 1));
    assert!(settle_group_run(&mut groups, &tx, 1, landed, updates, Vec::new()).is_none());
    assert!(matches!(
        groups[1].outcomes.as_slice(),
        [EntryOutcome::Fetched(3)]
    ));
    assert_eq!(
        groups[1].bytes,
        Some(EntryBytes::whole_blob(3, 1)),
        "the landed paid and resumed tallies are kept"
    );
    assert_eq!(rx.try_recv().expect("a flush batch").fetched_bytes, 3);
}

/// A deterministic `len`-byte blob.
fn test_blob(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

/// One scripted holder of `blob`, paying provider `provider`. A holder
/// given a `blip` flag resets its first stream once, 256 KiB in, sets the
/// flag as it does, and then serves.
fn scripted_holder(
    blob: &[u8],
    provider: u8,
    blip: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> anyhow::Result<crate::StreamCandidate<crate::source::ScriptedSource>> {
    let ledger = Arc::new(crate::PoolLedger::new(crate::Cumulative::default()));
    let mut source = crate::source::ScriptedSource::new(blob.to_vec())?.paying(Arc::clone(&ledger));
    if let Some(fired) = blip {
        source = source.fault_once_after(256 * 1024, move || {
            fired.store(true, std::sync::atomic::Ordering::SeqCst);
            anyhow!("connection reset")
        });
    }
    Ok(crate::StreamCandidate::new(
        source,
        Arc::new(std::sync::Mutex::new(crate::PoolContext {
            pool_id: alloy::primitives::B256::ZERO,
            provider: Address::repeat_byte(provider),
            deposit: alloy::primitives::U256::from(u128::MAX),
            client_signer: Arc::new(PrivateKeySigner::random()),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: alloy::primitives::U256::ZERO,
            prior_amount: alloy::primitives::U256::ZERO,
            client_binding: None,
            capability: None,
        })),
        ledger,
    ))
}

/// Two scripted holders of `blob`; the first (0xA1) blips once and sets
/// `first_blip` when given one.
fn two_scripted_holders(
    blob: &[u8],
    first_blip: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> anyhow::Result<(
    crate::StaticSources<crate::source::ScriptedSource>,
    Vec<crate::Holder>,
)> {
    let sources = crate::StaticSources::new(vec![
        scripted_holder(blob, 0xA1, first_blip)?,
        scripted_holder(blob, 0xB2, None)?,
    ])?;
    let holders = sources.holders();
    Ok((sources, holders))
}

/// A funder that is never asked: every scripted deposit is huge.
fn no_topups() -> crate::source::FakeFunder {
    crate::source::FakeFunder::new(crate::Recovery::Unavailable)
}

/// `blob`'s BLAKE3 root, the hash a scripted holder serves it under.
fn root(blob: &[u8]) -> [u8; 32] {
    *blake3::hash(blob).as_bytes()
}

/// `len` bytes at `offset` of the entry's `.partial` beside `dest`: a
/// range entry that fetched only some ranges stays unpromoted.
fn read_range(dest: &Path, offset: u64, len: u64) -> anyhow::Result<Vec<u8>> {
    use std::io::{Read as _, Seek as _};
    let mut f = std::fs::File::open(partial_path(dest))?;
    f.seek(std::io::SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; usize::try_from(len)?];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// A range-dedup entry fetches only the ranges no donor supplies, through
/// the acquire loop. Its one blipping holder cools while the other holder
/// carries the range, and the entry stays a `.partial` for the splice.
#[tokio::test(start_paused = true)]
async fn a_range_entry_survives_a_holder_blip() -> anyhow::Result<()> {
    let blob = test_blob(6 * 1024 * 1024);
    let ranges = vec![(1024 * 1024, 2 * 1024 * 1024)];
    let blipped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (provider, holders) = two_scripted_holders(&blob, Some(Arc::clone(&blipped)))?;
    let staging = tempfile::tempdir()?;
    let dest = staging.path().join("entry");
    let downloader = Downloader::new(&provider, no_topups())
        .holders(holders)
        .max_lanes(2);
    let stop = StopPolicy::new(
        false,
        Some(std::time::Duration::from_mins(1)),
        Arc::default(),
    );
    downloader
        .fetch_to_paths_until(
            &[DownloadTarget {
                hash: root(&blob),
                total_bytes: u64::try_from(blob.len())?,
                dest: &dest,
                ranges: Some(&ranges),
            }],
            None,
            &stop,
        )
        .await?;
    assert!(
        blipped.load(std::sync::atomic::Ordering::SeqCst),
        "holder 0xA1 reset a stream mid-range"
    );
    assert_eq!(
        read_range(&dest, 1024 * 1024, 2 * 1024 * 1024)?,
        blob[1024 * 1024..3 * 1024 * 1024]
    );
    assert!(
        !dest.exists(),
        "an entry with donor bytes left is not promoted"
    );
    Ok(())
}

/// An entry whose blob differs in size from its manifest lands, and the run
/// names it on one warning line.
#[test]
fn a_manifest_size_mismatch_warns() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("big.bin"), b"twelve bytes").expect("write");
    std::fs::write(tmp.path().join("ok.bin"), b"four").expect("write");
    let entries = [
        ManifestEntry::new("big.bin", [0xaa; 32], Some(4)),
        ManifestEntry::new("ok.bin", [0xbb; 32], Some(4)),
    ];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let outcomes = vec![EntryOutcome::Fetched(12), EntryOutcome::Fetched(4)];
    let updates = build_completed_updates(&refs, &outcomes, TEST_BUNDLE, tmp.path());
    assert_eq!(
        size_warnings(&refs, &updates),
        vec!["big.bin: manifest says 4 bytes, the blob is 12 bytes".to_string()]
    );
}

/// The run's report tallies every outcome: one failure per failed entry, in
/// order, a whole-file reuse counted once with its size, and the byte totals.
#[test]
fn the_report_tallies_every_outcome() {
    let outcomes = vec![
        EntryOutcome::Fetched(10),
        EntryOutcome::Linked,
        EntryOutcome::Skipped,
        EntryOutcome::Deduped(7),
        EntryOutcome::Failed {
            path: "x".into(),
            err: "boom".into(),
        },
    ];
    let transfer = Transfer {
        downloaded: 4,
        reconstructed: 20,
        resumed: 6,
    };
    let dedup = DedupSummary {
        spliced_bytes: 3,
        hints_ignored: 1,
    };
    let report = bundle_report(&outcomes, transfer, dedup, vec!["w".into()]);
    assert_eq!(
        (report.fetched, report.linked, report.skipped, report.failed),
        (1, 1, 1, 1)
    );
    assert_eq!((report.reused, report.reused_bytes), (1, 7));
    assert_eq!(
        (
            report.downloaded,
            report.reconstructed,
            report.resumed_bytes,
            report.spliced_bytes,
            report.hints_ignored
        ),
        (4, 20, 6, 3, 1)
    );
    assert_eq!(
        report.failures,
        vec![EntryFailure {
            path: "x".into(),
            error: "boom".into(),
        }]
    );
    assert_eq!(report.warnings, vec!["w".to_string()]);
}

/// Each pull reports only its own range-dedup counters: reading them for one
/// pull's report resets them for the next pull of the same puller.
#[test]
fn dedup_counters_are_per_pull() {
    let stats = DedupStats::default();
    stats.spliced_bytes.fetch_add(10, Ordering::Relaxed);
    stats.hints_ignored.fetch_add(2, Ordering::Relaxed);
    let first = stats.take();
    assert_eq!((first.spliced_bytes, first.hints_ignored), (10, 2));
    let second = stats.take();
    assert_eq!((second.spliced_bytes, second.hints_ignored), (0, 0));
}

/// A group's share of the run's outstanding work goes as its bytes land,
/// never past its plan, and whatever is left goes when the group settles.
#[test]
fn a_work_share_takes_its_bytes_off_the_run() {
    let outstanding = Arc::new(RunDemand::default());
    outstanding.plan(1_000);
    let share = WorkShare::new(
        Arc::clone(&outstanding),
        PlannedWork {
            download: 300,
            resumed: 0,
        },
    );
    share.delivered(100);
    assert_eq!(outstanding.outstanding(), 900);
    // Every fetch reports its whole position, so a repeat takes nothing more.
    share.delivered(100);
    assert_eq!(outstanding.outstanding(), 900);
    // Bytes past the plan take nothing past it.
    share.delivered(500);
    assert_eq!(outstanding.outstanding(), 700);
    drop(share);
    assert_eq!(outstanding.outstanding(), 700);

    let unfetched = WorkShare::new(
        Arc::clone(&outstanding),
        PlannedWork {
            download: 200,
            resumed: 0,
        },
    );
    drop(unfetched);
    assert_eq!(
        outstanding.outstanding(),
        500,
        "a group that settles without fetching plans no more work"
    );
}

/// A resumed group's fetch reports its whole position, the prefix an earlier
/// run left included: only what lands past that prefix comes off the run.
#[test]
fn a_resumed_share_counts_only_this_runs_bytes() {
    let outstanding = Arc::new(RunDemand::default());
    outstanding.plan(1_000);
    let share = WorkShare::new(
        Arc::clone(&outstanding),
        PlannedWork {
            download: 600,
            resumed: 400,
        },
    );
    share.delivered(400);
    assert_eq!(
        outstanding.outstanding(),
        1_000,
        "the prefix was paid before"
    );
    share.delivered(700);
    assert_eq!(outstanding.outstanding(), 700);
    drop(share);
    assert_eq!(outstanding.outstanding(), 400);
}

/// The pre-pass plans no work for a group whose every destination is
/// present, nor for one a whole-file donor supplies: neither counts toward
/// the run's outstanding work while it waits for a jobs slot.
#[tokio::test]
async fn planned_work_leaves_out_skipped_and_donor_groups() {
    let tmp = tempfile::tempdir().expect("tmp");
    let entries = vec![
        ManifestEntry::new("fetch.bin", [1; 32], Some(100)),
        ManifestEntry::new("present.bin", [2; 32], Some(200)),
        ManifestEntry::new("donor.bin", [3; 32], Some(300)),
    ];
    let mut disk = DiskState::default();
    disk.skip.insert("present.bin".to_string(), [2; 32]);
    disk.whole_file
        .insert([3; 32], tmp.path().join("elsewhere.bin"));
    let plan = build_fetch_plan(&entries);
    let work = planned_work(
        &entries,
        &Landing::open(tmp.path()).unwrap(),
        false,
        &plan,
        &disk,
    )
    .await;
    assert_eq!(
        work.get(&[1; 32]),
        Some(&PlannedWork {
            download: 100,
            resumed: 0,
        })
    );
    assert_eq!(work.len(), 1, "{work:?}");

    let all = planned_work(
        &entries,
        &Landing::open(tmp.path()).unwrap(),
        true,
        &plan,
        &DiskState::default(),
    )
    .await;
    assert_eq!(all.len(), 3, "an overwriting pull fetches every group");
}

/// Colliding entries plan no work, overwrite or not: they fail before their
/// fetch, so the run never quotes or pays for their bytes.
#[tokio::test]
async fn planned_work_leaves_out_colliding_entries() {
    let tmp = tempfile::tempdir().expect("tmp");
    let entries = vec![
        ManifestEntry::new("p", [1; 32], Some(100)),
        ManifestEntry::new("P", [2; 32], Some(200)),
        ManifestEntry::new("q", [3; 32], Some(300)),
    ];
    let plan = build_fetch_plan(&entries);
    let landing = Landing::open(tmp.path()).unwrap();
    for overwrite in [false, true] {
        let disk =
            resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), overwrite).await;
        let work = planned_work(&entries, &landing, overwrite, &plan, &disk).await;
        assert_eq!(
            work.keys().collect::<Vec<_>>(),
            vec![&[3; 32]],
            "overwrite={overwrite}"
        );
    }
}
