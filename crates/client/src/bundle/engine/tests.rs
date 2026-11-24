use super::super::{BundleManifest, MAX_MANIFEST_BYTES, MalformedManifest, NotAManifest};
use super::*;
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
    let chain = crate::session::test_support::one_chain(
        format!("http://{}", rpc.local_addr()?),
        1,
        Address::repeat_byte(0x33),
        Address::repeat_byte(0x55),
    );
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
    let report = bundle_report(&run.outcomes, Transfer::default(), Vec::new());
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

/// A pool's pass leaves work for the next pool's pass when an entry failed or
/// did not run, or a fault ended it; a pass that landed every entry, or that
/// its caller interrupted, leaves none.
#[test]
fn a_pass_leaves_work_for_the_next_pool_only_when_an_entry_is_left() {
    let pass = |outcomes, stopped: Option<anyhow::Error>, interrupted| PullRun {
        outcomes,
        warnings: Vec::new(),
        transfer: Transfer::default(),
        interrupted,
        stopped,
    };
    let landed = || vec![EntryOutcome::Fetched(5), EntryOutcome::Skipped];
    assert!(!leaves_work(&pass(landed(), None, false)));
    assert!(leaves_work(&pass(
        vec![
            EntryOutcome::Fetched(5),
            EntryOutcome::failed("b.bin", &anyhow!("no holder takes this pool")),
        ],
        None,
        false
    )));
    assert!(leaves_work(&pass(
        vec![EntryOutcome::Fetched(5), EntryOutcome::NotRun],
        None,
        false
    )));
    assert!(leaves_work(&pass(landed(), Some(give_up()), false)));
    assert!(!leaves_work(&pass(
        vec![EntryOutcome::NotRun],
        Some(give_up()),
        true
    )));
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

/// A manifest fetch that ends on the manifest cap says that the blob is not a
/// manifest and how to fetch it, and keeps the typed fault for a caller.
#[test]
fn a_capped_manifest_fetch_points_at_the_raw_blob() {
    let too_large = crate::BlobTooLarge {
        reached: 64 << 20,
        ceiling: 64 << 20,
    };
    let err = manifest_fetch_error(anyhow::Error::new(too_large));
    let text = format!("{err:#}");
    assert!(text.starts_with("fetch bundle manifest blob: "), "{text}");
    assert!(
        text.contains("larger than a bundle manifest can be (64 MiB"),
        "{text}"
    );
    assert!(text.contains("drop the trailing path"), "{text}");
    assert!(err.downcast_ref::<crate::BlobTooLarge>().is_some());
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
    let report = bundle_report(&outcomes, Transfer::default(), Vec::new());
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
            landed_from: None,
        },
        ManifestEntry {
            path: "bad.txt".into(),
            hash: [0xcc; 32],
            size: Some(9),
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
            landed_from: None,
        },
        ManifestEntry {
            path: "skipped.txt".into(),
            hash: [0x22; 32],
            size: Some(4),
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
            landed_from: None,
        },
        ManifestEntry {
            path: "pending.txt".into(),
            hash: [0x44; 32], // new hash, not yet fetched
            size: Some(3),
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

const GROUP: u64 = decdn_bao_range::CHUNK_GROUP_BYTES;

/// A sized entry with a whole-file hash of `[whole; 32]`.
fn mentry(whole: u8, size: u64) -> ManifestEntry {
    ManifestEntry {
        path: format!("e{whole:02x}"),
        hash: [whole; 32],
        size: Some(size),
        landed_from: None,
    }
}

/// Groups are scheduled smallest whole-file first (unsized last), tie-broken
/// by hash.
#[test]
fn groups_ordered_smallest_first_stable() {
    let big = mentry(0x01, 900);
    let small = mentry(0x02, 10);
    let mid = mentry(0x03, 100);
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
        landed_from: None,
    };
    let sized_dup = ManifestEntry {
        path: "b".into(),
        hash: whole,
        size: Some(10),
        landed_from: None,
    };
    let big = mentry(0x01, 900);
    let refs = vec![&big, &unsized_first, &sized_dup];
    let ordered = order_groups_smallest_first(group_by_hash(&refs));
    let sizes: Vec<_> = ordered
        .iter()
        .map(|g| g.entries.iter().find_map(|e| e.size))
        .collect();
    assert_eq!(sizes, vec![Some(10), Some(900)]);
}

/// A run's rate hints take this run's discovery sample over the peer store,
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
/// and each pays its whole blob with its bao proof tree on the wire.
#[test]
fn planned_downloads_list_each_blob_once() {
    let a = mentry(0x0a, 3 * GROUP);
    let total_b = 3 * GROUP + 50_000;
    let b = mentry(0x0b, total_b);
    let mut a_again = a.clone();
    a_again.path = "again".into();
    let unsized_blob = ManifestEntry {
        path: "unsized".into(),
        hash: [0x0c; 32],
        size: None,
        landed_from: None,
    };
    let entries = [a.clone(), b.clone(), a_again, unsized_blob];
    let planned = planned_downloads(&entries);
    let rows: Vec<_> = planned.iter().map(|p| (p.hash, p.size)).collect();
    assert_eq!(
        rows,
        vec![
            (a.hash, Some(3 * GROUP)),
            (b.hash, Some(total_b)),
            ([0x0c; 32], None),
        ]
    );
    let wire = |total| {
        decdn_bao_range::align_range_clamped(0, total, total)
            .unwrap()
            .wire_len()
    };
    assert_eq!(planned[0].wire, wire(3 * GROUP));
    assert_eq!(planned[1].wire, wire(total_b));
    assert!(planned[1].wire > total_b, "{:?}", planned[1]);
    assert_eq!(planned[2].wire, 0);
}

/// The whole-blob path reads what an earlier run fetched from the run's
/// store and pays only for the rest (#2236). No store resumes nothing.
#[tokio::test]
async fn resumed_spans_read_the_store_an_earlier_run_left() {
    let tmp = tempfile::tempdir().expect("tmp");
    let content: Vec<u8> = (0..3 * GROUP + 99)
        .map(|i| u8::try_from(i % 251).expect("i % 251 fits in u8"))
        .collect();
    let len = u64::try_from(content.len()).expect("fits u64");
    let hash = *blake3::hash(&content).as_bytes();

    assert!(
        resumed_spans(None, hash).await.is_empty(),
        "no store, no spans"
    );

    // An earlier, interrupted run fetched `[0, GROUP)` into the store.
    let dir = BlobDir::open(&tmp.path().join("store"))
        .await
        .expect("open store");
    dir.blob(hash, len, None)
        .await
        .expect("open entry")
        .admit_prefix(&content, GROUP)
        .await
        .expect("seed the store");
    let resumed = resumed_spans(Some(&dir), hash).await;
    dir.shutdown().await.expect("shut down store");
    assert_eq!(resumed, vec![(0, GROUP)]);
    assert_eq!(
        EntryBytes::whole_blob(len, span_bytes(&resumed)),
        EntryBytes {
            paid: len - GROUP,
            resumed: GROUP,
        }
    );
    // A resumed count never exceeds the blob.
    assert_eq!(EntryBytes::whole_blob(10, 20).paid, 0);
    assert_eq!(EntryBytes::whole_blob(10, 20).resumed, 10);
}

/// `hash_file` streams the whole file and returns its BLAKE3 hash, matching
/// a direct in-memory hash of the same bytes.
#[test]
fn hash_file_matches_direct_blake3_of_file_contents() {
    let tmp = tempfile::tempdir().expect("tmp");
    let p = tmp.path().join("f");
    let data: Vec<u8> = (0..5000u32)
        .map(|i| u8::try_from(i % 256).expect("i % 256 fits in u8"))
        .collect();
    std::fs::write(&p, &data).expect("write");

    let got = hash_file(&p).expect("hash_file");
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

/// One malformed entry hash makes the whole manifest [`MalformedManifest`],
/// and the error names the entry.
#[test]
fn parse_manifest_rejects_a_malformed_entry_hash() {
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

/// Segments that Windows joins with another name, opens as a device or
/// stream, or that would drive a terminal, are refused on every platform
/// (#2746, #2705). Names that only look like them are not.
const UNPORTABLE_SEGMENTS: [&str; 18] = [
    "README.",
    "README ",
    "README::$DATA",
    "a:b",
    "a\\b",
    "a|b",
    "what?",
    "x\u{1b}[31m",
    "line\nbreak",
    "c1\u{9b}",
    "con",
    "NUL.txt",
    "com1.tar.gz",
    "LPT9 .log",
    "COM\u{b9}",
    "lpt\u{b3}.txt",
    "LONGFI~1.TXT",
    "dir~12",
];
const PORTABLE_SEGMENTS: [&str; 13] = [
    "README",
    "README.md",
    ".hidden",
    "..foo",
    "v1.2",
    "console.txt",
    "com.txt",
    "COM10",
    "COM\u{b9}\u{b2}",
    "foo~bar",
    "file~",
    "~1",
    "caf\u{e9} \u{1f600}.txt",
];

#[test]
fn path_segment_problem_refuses_unportable_names_only() {
    for seg in ["", ".", ".."].into_iter().chain(UNPORTABLE_SEGMENTS) {
        assert!(
            super::super::path_segment_problem(seg).is_some(),
            "{seg:?} must be refused"
        );
    }
    for seg in PORTABLE_SEGMENTS {
        assert_eq!(
            super::super::path_segment_problem(seg),
            None,
            "{seg:?} must be accepted"
        );
    }
}

/// An entry path with an unportable segment, at any depth, makes the whole
/// manifest [`MalformedManifest`], and the error escapes the segment.
#[test]
fn parse_manifest_rejects_an_unportable_path_segment() {
    let good = format_hash([0xab; 32]);
    for seg in UNPORTABLE_SEGMENTS {
        for path in [seg.to_string(), format!("dir/{seg}/file")] {
            let manifest = serde_json::json!({"version": 1, "entries": [
                {"path": "ok", "hash": good},
                {"path": path, "hash": good},
            ]});
            let err = parse_json(&manifest).unwrap_err();
            assert!(err.is::<MalformedManifest>(), "{manifest}: {err:#}");
            let msg = format!("{err:#}");
            assert!(
                msg.contains(&format!("unsafe path segment {seg:?}")),
                "{manifest}: {msg}"
            );
            assert!(!msg.chars().any(char::is_control), "{msg:?}");
        }
    }
    let portable: Vec<_> = PORTABLE_SEGMENTS
        .iter()
        .map(|seg| serde_json::json!({"path": format!("d/{seg}"), "hash": good}))
        .collect();
    let parsed = parse_json(&serde_json::json!({"version": 1, "entries": portable}))
        .expect("portable names parse");
    assert_eq!(parsed.entries.len(), PORTABLE_SEGMENTS.len());
}

/// A manifest built without `parse` still cannot land an unportable path:
/// [`safe_join`] refuses it.
#[test]
fn safe_join_rejects_an_unportable_segment() {
    for seg in UNPORTABLE_SEGMENTS {
        let err = safe_join(Path::new("/out"), &format!("dir/{seg}")).unwrap_err();
        assert!(
            format!("{err:#}").contains(&format!("{seg:?}")),
            "{seg:?}: {err:#}"
        );
    }
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

/// A blob an earlier run already finalized in staging pays nothing, while
/// its copy on disk is still a whole file.
#[test]
fn group_transfer_of_a_blob_already_in_staging_downloads_nothing() {
    let t = group_transfer(&[EntryOutcome::Fetched(100)], Some(EntryBytes::default()));
    assert_eq!(t.downloaded, 0);
    assert_eq!(t.reconstructed, 100);
}

/// A blob resumed from an earlier run's store downloads only what this
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

/// An unsized entry at `path` whose hash is `[hash; 32]`.
fn entry(path: &str, hash: u8) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        hash: [hash; 32],
        size: None,
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

/// Every case spelling of a reserved pull-state name is reserved: on a file
/// system that ignores case, `.DECDN-PARTIAL` is the staging directory. A
/// path that only starts with a reserved name, or nests the skip-cache name
/// below the root, stays writable.
#[test]
fn writable_dest_reserves_every_case_of_the_pull_state_names() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for reserved in [
        ".DECDN-PARTIAL/deadbeef".to_owned(),
        ".Decdn-Partial".to_owned(),
        ".DECDN-MANIFEST.JSON".to_owned(),
        ".Decdn-Bundles/x.json".to_owned(),
        format!("{STAGING_DIR}/x"),
    ] {
        assert!(
            writable_dest(root, &reserved).is_err(),
            "{reserved} must be reserved"
        );
    }
    for writable in [
        ".decdn-partial-notes/x",
        "sub/.decdn-manifest.json",
        "sub/.DECDN-PARTIAL/x",
        ".decdn-manifest.json.bak",
    ] {
        assert!(
            writable_dest(root, writable).is_ok(),
            "{writable} must stay writable"
        );
    }
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
                landing.move_into_place(&staging, &dest)
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
/// so it must never be indexed as a whole-file donor.
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
        landed_from: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains_key("link.txt"));
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
    let report = bundle_report(&outcomes, transfer, vec!["w".into()]);
    assert_eq!(
        (report.fetched, report.linked, report.skipped, report.failed),
        (1, 1, 1, 1)
    );
    assert_eq!((report.reused, report.reused_bytes), (1, 7));
    assert_eq!(
        (
            report.downloaded,
            report.reconstructed,
            report.resumed_bytes
        ),
        (4, 20, 6)
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
    let work = planned_work(
        &entries,
        &Landing::open(tmp.path()).unwrap(),
        false,
        &disk,
        None,
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
        &DiskState::default(),
        None,
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
    let landing = Landing::open(tmp.path()).unwrap();
    for overwrite in [false, true] {
        let disk =
            resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), overwrite).await;
        let work = planned_work(&entries, &landing, overwrite, &disk, None).await;
        assert_eq!(
            work.keys().collect::<Vec<_>>(),
            vec![&[3; 32]],
            "overwrite={overwrite}"
        );
    }
}

/// A manifest fetch never runs under the run-wide blob cap: a 1 TiB run cap,
/// or none, still caps the manifest at the largest manifest, and a lower run
/// cap stays.
#[test]
fn a_manifest_fetch_runs_under_the_manifest_cap() {
    let manifest_cap = u64::try_from(MAX_MANIFEST_BYTES).unwrap();
    assert_eq!(manifest_cap, MAX_MANIFEST_MB << 20);
    assert_eq!(manifest_cap_bytes(1 << 40), manifest_cap);
    assert_eq!(manifest_cap_bytes(0), manifest_cap);
    assert_eq!(manifest_cap_bytes(1 << 20), 1 << 20);
}
