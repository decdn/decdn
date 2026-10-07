use super::*;

/// Every entry draws its own sample of the registry, so a node that one
/// draw leaves out still reaches other entries.
#[test]
fn entry_candidates_resample_the_registry_for_each_entry() {
    let registry: Vec<NodeCandidate> = (1u8..=7).map(|b| stripe_candidate(b, b)).collect();
    let mut seen = HashSet::new();
    let mut draws = HashSet::new();
    for _ in 0..200 {
        let sample = entry_candidates(&registry, None);
        assert_eq!(sample.len(), discovery::SELECT_K);
        let ids: std::collections::BTreeSet<PublicKey> = sample.iter().map(|c| c.node_id).collect();
        seen.extend(ids.iter().copied());
        draws.insert(ids);
    }
    assert_eq!(seen.len(), registry.len(), "every node reaches some entry");
    assert!(draws.len() > 1, "entries draw different samples");
}

/// A peer store holding `n` identities confirmed at `now`.
fn store_with_fresh_identities(dir: &Path, n: u8, now: u64) -> anyhow::Result<()> {
    let store = decdn_client::PeerStore::open(dir);
    for b in 1..=n {
        store.upsert_identity(&stripe_candidate(b, b), now)?;
    }
    Ok(())
}

/// Enough identity-fresh records stand in for the registry; too few, or
/// records past the refresh horizon, leave the run to a live read.
#[test]
fn cached_registry_needs_enough_fresh_identities() -> anyhow::Result<()> {
    let cfg = decdn_client::StoreConfig::default();
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

/// A local listener stands in for the RPC endpoint, and a pull's resolved
/// chain points at it, with `extra` flags appended.
fn chain_against(
    rpc: &std::net::TcpListener,
    data_dir: &Path,
    extra: &[&str],
) -> anyhow::Result<(ClientFetchArgs, fetch::ResolvedChain)> {
    use clap::Parser as _;
    #[derive(clap::Parser)]
    struct T {
        #[command(flatten)]
        args: BundlePullArgs,
    }
    let rpc_url = format!("http://{}", rpc.local_addr()?);
    let data_dir = data_dir.display().to_string();
    let base = [
        "t",
        "-o",
        "out",
        "--hash",
        "b3:aa",
        "--rpc-url",
        &rpc_url,
        "--payment-pool-address",
        "0x3333333333333333333333333333333333333333",
        "--slash-judge-address",
        "0x4444444444444444444444444444444444444444",
        "--capacity-bond-address",
        "0x5555555555555555555555555555555555555555",
        "--data-dir",
        &data_dir,
        "--timeout-ms",
        "200",
    ];
    let common = T::try_parse_from(base.iter().chain(extra))?.args.common;
    let chain = fetch::resolve_chain(&common, &decdn_common::config::FileConfig::default())?;
    Ok((common, chain))
}

/// With fresh identities cached, discovery opens no connection to the RPC
/// endpoint. `--rediscover` forces the live read.
#[tokio::test]
async fn discover_candidates_skips_the_registry_read_on_fresh_identities() -> anyhow::Result<()> {
    let min = u8::try_from(decdn_client::StoreConfig::default().min_fresh_candidates)?;
    let dir = tempfile::tempdir()?;
    store_with_fresh_identities(dir.path(), min, fetch::now_secs_cli())?;
    let rpc = std::net::TcpListener::bind("127.0.0.1:0")?;
    rpc.set_nonblocking(true)?;

    let (common, chain) = chain_against(&rpc, dir.path(), &[])?;
    let found = discover_candidates(&chain, &common).await?;
    assert_eq!(found.len(), usize::from(min));
    assert!(
        matches!(rpc.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "a cached run must not reach the RPC endpoint"
    );

    let (common, chain) = chain_against(&rpc, dir.path(), &["--rediscover"])?;
    // The listener never answers, so the read times out and falls back to
    // the same store. Only the connection attempt matters here.
    drop(discover_candidates(&chain, &common).await);
    assert!(rpc.accept().is_ok(), "--rediscover reads the registry");
    Ok(())
}

/// A blob no provider holds is its entry's fault: it fails its own entry
/// and leaves the rest of the pull running.
#[test]
fn a_blob_no_holder_has_fails_only_its_entry() {
    let err = anyhow::Error::new(decdn_client::NoSourceHasBlob);
    assert_eq!(entry_scope(&err), decdn_client::FatalScope::Item);
    assert!(!ends_the_pull(&err));
}

/// A voucher rejection is the one pool's, so every entry would meet it.
#[test]
fn a_voucher_rejection_ends_the_whole_pull() {
    let err = anyhow::Error::new(decdn_client::UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    })
    .context("entry");
    assert_eq!(entry_scope(&err), decdn_client::FatalScope::Command);
    assert!(ends_the_pull(&err));
}

/// A capability signer every provider refuses, whatever its rate, ends
/// the whole pull: every entry would meet it. One that only the providers
/// of an entry refuse at their rates fails that entry alone, since a
/// cheaper provider of another entry can still serve it (#2338).
#[test]
fn a_drained_signer_ends_the_pull_only_at_every_rate() {
    let drained = |remaining| decdn_client::SignerCapDrained {
        pool_id: alloy::primitives::B256::ZERO,
        signer: Address::ZERO,
        provider: Address::ZERO,
        remaining,
        rate_per_mb: 10,
        expired: false,
    };
    let everywhere = anyhow::Error::new(drained(0)).context("entry");
    assert_eq!(entry_scope(&everywhere), decdn_client::FatalScope::Command);
    assert!(ends_the_pull(&everywhere));
    let here = anyhow::Error::new(drained(5))
        .context(decdn_client::NoSourceServesSigner)
        .context("entry");
    assert_eq!(entry_scope(&here), decdn_client::FatalScope::Item);
    assert!(!ends_the_pull(&here));
}

/// A blob over the client's cap fails only its own entry.
#[test]
fn an_over_cap_blob_fails_only_its_entry() {
    let err = anyhow::Error::new(decdn_client::BlobTooLarge {
        reached: 2,
        ceiling: 1,
    });
    assert_eq!(entry_scope(&err), decdn_client::FatalScope::Item);
    assert!(!ends_the_pull(&err));
}

/// A give-up is item-scoped, but the clock is the command's, so it ends
/// the pull: every other entry would give up at the same moment.
#[test]
fn a_give_up_ends_the_whole_pull() {
    let err = anyhow::Error::new(decdn_client::GaveUp {
        idle: std::time::Duration::from_secs(1),
    });
    assert_eq!(entry_scope(&err), decdn_client::FatalScope::Item);
    assert!(ends_the_pull(&err));
    assert!(!ends_the_pull(&anyhow!("stream reset")));
}

#[test]
fn entry_retries_flag_is_gone() {
    use clap::Parser as _;
    #[derive(clap::Parser)]
    struct T {
        #[command(flatten)]
        args: BundlePullArgs,
    }
    let base = ["t", "-o", "out", "--hash", "b3:aa"];
    assert!(T::try_parse_from(base).is_ok());
    let parsed = T::try_parse_from(base.iter().copied().chain(["--entry-retries", "2"]));
    assert!(parsed.is_err());
}

/// A command-wide fault stops the pull: no group after it starts, and the
/// fault is what the pull ends with.
#[tokio::test]
async fn a_command_fault_stops_every_later_entry() {
    let started = std::sync::Mutex::new(Vec::new());
    let mut settled = Vec::new();
    let stopped = run_groups(
        vec![0u8, 1, 2, 3],
        1,
        |i| {
            started.lock().unwrap().push(i);
            async move {
                if i == 1 {
                    Err(anyhow::Error::new(decdn_client::UpstreamVoucherRejected {
                        reason: decdn_protocol::client::VoucherRejectReason::PoolExhausted,
                        bundle: None,
                        proof_generation: None,
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
    )
    .await;
    let err = stopped.expect("the pull stops");
    assert!(
        err.downcast_ref::<decdn_client::UpstreamVoucherRejected>()
            .is_some()
    );
    assert_eq!(*started.lock().unwrap(), vec![0, 1]);
    assert_eq!(settled, vec![0, 1]);
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
        run_groups(vec![0u8, 1, 2, 3], 4, run, |_, r| r.err()),
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
    assert!(
        matches!(poll!(retake.as_mut()), Poll::Ready(Ok(()))),
        "the retake comes before d"
    );
    assert!(poll!(d.as_mut()).is_pending());
    assert!(waiter.held());
}

/// A command-wide fault drops the items still in flight, and each dropped
/// item's drop guard runs: a fetch dropped this way still persists its
/// lanes' watermarks (`fetch::SettleOnDrop` in `acquire_entry`).
#[tokio::test]
async fn a_command_fault_settles_the_items_it_drops() {
    let settled = std::sync::atomic::AtomicU32::new(0);
    let stopped = run_groups(
        vec![0u8, 1],
        2,
        |i| {
            let settled = &settled;
            async move {
                if i == 1 {
                    return Err(anyhow::Error::new(decdn_client::UpstreamVoucherRejected {
                        reason: decdn_protocol::client::VoucherRejectReason::PoolExhausted,
                        bundle: None,
                        proof_generation: None,
                    }));
                }
                // In flight when its sibling fails: it never finishes.
                let on_drop = fetch::SettleOnDrop::new(|| {
                    settled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
                std::future::pending::<()>().await;
                on_drop.disarm();
                Ok(())
            }
        },
        |_, r: anyhow::Result<()>| r.err().filter(ends_the_pull),
    )
    .await;
    assert!(stopped.is_some(), "the sibling's fault ends the run");
    assert_eq!(
        settled.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the dropped item settled on drop"
    );
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
                Err(anyhow::Error::new(decdn_client::BlobTooLarge {
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
    let stop = decdn_client::StopPolicy::new(
        false,
        Some(limit),
        Arc::new(decdn_client::ProgressClock::new()),
    );
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
    )
    .await;
    assert!(
        stopped
            .as_ref()
            .is_some_and(|e| e.downcast_ref::<decdn_client::GaveUp>().is_some())
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
            hash: "b3:aa".into(),
            size: Some(4),
            chunks: Some(vec![ManifestChunk {
                hash: "b3:bb".into(),
                size: 4,
            }]),
        },
        ManifestEntry {
            path: "bad.txt".into(),
            hash: "b3:cc".into(),
            size: Some(9),
            chunks: None,
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
    let upd = build_completed_updates(&refs, &outcomes, tmp.path());
    assert!(upd.contains_key("ok.txt"));
    let rec = upd.get("ok.txt").expect("rec");
    assert_eq!(rec.hash, "b3:aa");
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
            hash: "b3:l".into(),
            size: Some(2),
            chunks: None,
        },
        ManifestEntry {
            path: "skipped.txt".into(),
            hash: "b3:s".into(),
            size: Some(4),
            chunks: None,
        },
    ];
    let outcomes = vec![EntryOutcome::Linked, EntryOutcome::Skipped];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let upd = build_completed_updates(&refs, &outcomes, tmp.path());
    // Both are successes this run and present on disk → both recorded.
    assert_eq!(upd.get("linked.txt").expect("linked").hash, "b3:l");
    assert_eq!(upd.get("skipped.txt").expect("skipped").hash, "b3:s");
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
            hash: "b3:done".into(),
            size: Some(3),
            chunks: None,
        },
        ManifestEntry {
            path: "pending.txt".into(),
            hash: "b3:new".into(), // new hash, not yet fetched
            size: Some(3),
            chunks: None,
        },
    ];
    let outcomes = vec![EntryOutcome::Fetched(3)]; // only "done.txt" has completed
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let upd = build_completed_updates(&refs, &outcomes, tmp.path());
    assert!(upd.contains_key("done.txt"));
    assert!(
        !upd.contains_key("pending.txt"),
        "an entry not yet in outcomes must never be recorded"
    );
}

fn saved_file(hash: &str, size: u64) -> bundle_manifest::SavedFile {
    bundle_manifest::SavedFile {
        hash: hash.into(),
        size,
        mtime: SavedMtime { secs: 1, nanos: 0 },
        chunks: None,
    }
}

fn one_update(path: &str, hash: &str, size: u64) -> BTreeMap<String, bundle_manifest::SavedFile> {
    let mut m = BTreeMap::new();
    m.insert(path.to_string(), saved_file(hash, size));
    m
}

// A Ctrl-C mid-pull drops the drive while an entry is still in flight. The
// flush must still write every entry that settled before it.
#[tokio::test]
async fn an_interrupted_drive_still_flushes_the_settled_entries() {
    let tmp = tempfile::tempdir().expect("tmp");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (fire, mut interrupt) = Interrupt::manual();
    let drive = async move {
        tx.send(FlushBatch {
            updates: one_update("done.bin", "b3:done", 1),
            fetched_bytes: 1,
        })
        .expect("send");
        // Ctrl-C while another entry is still in flight: it never settles.
        fire.send(()).expect("fire");
        std::future::pending::<()>().await;
    };
    let flush = flush_task(tmp.path(), SavedManifest::default(), rx);

    assert!(drive_until_interrupted(drive, flush, &mut interrupt).await);
    let saved = bundle_manifest::load(tmp.path());
    assert_eq!(saved.get("done.bin").expect("done").hash, "b3:done");
}

#[tokio::test]
async fn a_drive_that_finishes_is_not_interrupted() {
    let tmp = tempfile::tempdir().expect("tmp");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<FlushBatch>();
    let drive = async move { drop(tx) };
    let (_fire, mut interrupt) = Interrupt::manual();
    let flush = flush_task(tmp.path(), SavedManifest::default(), rx);
    assert!(!drive_until_interrupted(drive, flush, &mut interrupt).await);
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

    let saved = bundle_manifest::load(tmp.path());
    assert_eq!(saved.get("a.bin").expect("a").hash, "b3:a");
    assert_eq!(saved.get("b.bin").expect("b").hash, "b3:b");
}

// The flush folds onto the prior skip-cache: entries a prior run recorded but
// this run never touched survive, alongside this run's new records.
#[tokio::test]
async fn flush_task_keeps_prior_untouched_entries() {
    let tmp = tempfile::tempdir().expect("tmp");
    let mut prior = SavedManifest::default();
    bundle_manifest::merge(&mut prior, one_update("old.bin", "b3:old", 9));

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(FlushBatch {
        updates: one_update("new.bin", "b3:new", 2),
        fetched_bytes: 2,
    })
    .expect("send");
    drop(tx);
    flush_task(tmp.path(), prior, rx).await;

    let saved = bundle_manifest::load(tmp.path());
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
            if bundle_manifest::load(tmp.path()).get("big.bin").is_some() {
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

/// Minimal `ManifestEntry` for the `--select` tests: path + size, no chunks.
fn sel_entry(path: &str, size: u64) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        hash: "b3:00".into(),
        size: Some(size),
        chunks: None,
    }
}

#[test]
fn parse_selection_drops_commented_and_deleted_lines() {
    let entries = vec![
        sel_entry("a.bin", 1),
        sel_entry("b.bin", 2),
        sel_entry("c.bin", 3),
    ];
    // "a.bin" kept, "b.bin" commented out, "c.bin" deleted entirely.
    let edited = "# header\na.bin\t1 B\n#b.bin\t2 B\n";
    let kept = parse_selection(edited, entries).expect("parse");
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["a.bin"]);
}

#[test]
fn parse_selection_preserves_manifest_order_regardless_of_edit_order() {
    let entries = vec![sel_entry("a.bin", 1), sel_entry("b.bin", 2)];
    // User reordered the lines; output still follows manifest order.
    let edited = "b.bin\nb.bin is not a path\n".replace("b.bin is not a path", "a.bin");
    let kept = parse_selection(&edited, entries).expect("parse");
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["a.bin", "b.bin"]);
}

#[test]
fn parse_selection_empty_when_everything_removed() {
    let entries = vec![sel_entry("a.bin", 1)];
    let kept = parse_selection("# all gone\n", entries).expect("parse");
    assert!(kept.is_empty());
}

#[test]
fn parse_selection_rejects_unknown_path() {
    let entries = vec![sel_entry("a.bin", 1)];
    let err = parse_selection("a.bin\ntypo.bin\n", entries).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("typo.bin"), "{msg}");
}

#[test]
fn parse_selection_ignores_the_size_annotation_after_the_tab() {
    let entries = vec![sel_entry("weights.bin", 1500)];
    // The size column is arbitrary human text; only the path is read.
    let kept = parse_selection("weights.bin\t1.5 KB\n", entries).expect("parse");
    assert_eq!(kept.len(), 1);
}

#[test]
fn render_then_parse_unedited_keeps_every_entry() {
    let entries = vec![sel_entry("a.bin", 1), sel_entry("dir/b.bin", 2000)];
    let buffer = render_selection(&entries);
    let kept = parse_selection(&buffer, entries).expect("parse");
    let paths: Vec<&str> = kept.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, vec!["a.bin", "dir/b.bin"]);
}

#[test]
fn editor_command_prefers_visual_then_editor_then_vi() {
    assert_eq!(editor_command(Some("nano"), Some("vim")), vec!["nano"]);
    assert_eq!(editor_command(None, Some("vim")), vec!["vim"]);
    assert_eq!(editor_command(None, None), vec!["vi"]);
}

#[test]
fn editor_command_treats_blank_as_unset() {
    // A blank/whitespace-only $VISUAL falls through to $EDITOR.
    assert_eq!(editor_command(Some("   "), Some("vim")), vec!["vim"]);
    assert_eq!(editor_command(Some(""), None), vec!["vi"]);
}

#[test]
fn editor_command_splits_arguments() {
    assert_eq!(
        editor_command(Some("code --wait"), None),
        vec!["code", "--wait"]
    );
}

#[test]
fn check_selectable_rejects_hash_leading_and_tabbed_paths() {
    assert!(check_selectable(&[sel_entry("#weird.bin", 1)]).is_err());
    assert!(check_selectable(&[sel_entry("has\ttab.bin", 1)]).is_err());
    // Leading whitespace before a '#' would read as a comment and be
    // silently dropped by parse_selection — must be rejected here.
    assert!(check_selectable(&[sel_entry(" #weird.bin", 1)]).is_err());
    // Any leading/trailing whitespace cannot round-trip (parse trims it).
    assert!(check_selectable(&[sel_entry(" leading.bin", 1)]).is_err());
    assert!(check_selectable(&[sel_entry("trailing.bin ", 1)]).is_err());
    assert!(check_selectable(&[sel_entry("fine/name.bin", 1)]).is_ok());
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
        hash: format!("b3:{}", blake3::Hash::from_bytes([byte; 32]).to_hex()),
        size,
    }
}

/// A chunked entry's hints accumulate offsets by the running size sum, in
/// order, and validate that the chunk sizes reconstruct the whole-file size.
#[test]
fn hints_of_accumulates_offsets_and_validates_the_size_sum() {
    let e = ManifestEntry {
        path: "m.bin".into(),
        hash: format!("b3:{}", blake3::Hash::from_bytes([0xff; 32]).to_hex()),
        size: Some(100),
        chunks: Some(vec![chunk(60, 0xaa), chunk(40, 0xbb)]),
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
        hash: format!("b3:{}", blake3::Hash::from_bytes([0xff; 32]).to_hex()),
        size: Some(100),
        chunks: Some(vec![chunk(60, 0xaa), chunk(30, 0xbb)]),
    };
    assert!(hints_of(&e).is_none());
}

/// No whole-file `size` to validate against, or an unparseable chunk hash, both
/// drop back to a plain fetch.
#[test]
fn hints_of_needs_a_size_and_valid_chunk_hashes() {
    let no_size = ManifestEntry {
        path: "m.bin".into(),
        hash: "b3:ff".into(),
        size: None,
        chunks: Some(vec![chunk(60, 0xaa)]),
    };
    assert!(hints_of(&no_size).is_none());

    let bad_hash = ManifestEntry {
        path: "m.bin".into(),
        hash: "b3:ff".into(),
        size: Some(4),
        chunks: Some(vec![ManifestChunk {
            hash: "not-a-hash".into(),
            size: 4,
        }]),
    };
    assert!(hints_of(&bad_hash).is_none());
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
        hash: format!("b3:{}", blake3::Hash::from_bytes([whole; 32]).to_hex()),
        size: Some(size),
        chunks: Some(chunks.iter().map(|&(s, b)| chunk(s, b)).collect()),
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
    let whole = format!("b3:{}", blake3::Hash::from_bytes([0x42; 32]).to_hex());
    // Two paths for the same small blob; the first is unsized on the wire.
    let unsized_first = ManifestEntry {
        path: "a".into(),
        hash: whole.clone(),
        size: None,
        chunks: None,
    };
    let sized_dup = ManifestEntry {
        path: "b".into(),
        hash: whole,
        size: Some(10),
        chunks: None,
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
            let (dir, stem) = fetch::ranged_store_location(&self.staging).expect("location");
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

#[test]
fn parse_manifest_accepts_v1_with_optional_size() {
    let json = br#"{"version":1,"entries":[{"path":"a.txt","hash":"b3:ab","size":4},{"path":"b","hash":"b3:cd"}]}"#;
    let m = parse_manifest(json).unwrap();
    assert_eq!(m.entries.len(), 2);
    assert_eq!(m.entries[0].size, Some(4));
    assert_eq!(m.entries[1].size, None);
}

#[test]
fn parse_manifest_accepts_chunked_entry_in_order() {
    let json = br#"{"version":1,"entries":[{"path":"m.bin","hash":"b3:whole","size":100,"chunks":[{"hash":"b3:c0","size":60},{"hash":"b3:c1","size":40}]}]}"#;
    let m = parse_manifest(json).unwrap();
    let chunks = m.entries[0].chunks.as_ref().expect("chunked entry");
    let hashes: Vec<&str> = chunks.iter().map(|c| c.hash.as_str()).collect();
    assert_eq!(hashes, vec!["b3:c0", "b3:c1"]);
}

#[test]
fn parse_manifest_plain_entry_has_no_chunks() {
    let json = br#"{"version":1,"entries":[{"path":"a.txt","hash":"b3:ab","size":4}]}"#;
    let m = parse_manifest(json).unwrap();
    assert!(m.entries[0].chunks.is_none());
}

#[test]
fn parse_manifest_rejects_unsupported_version() {
    let json = br#"{"version":2,"entries":[]}"#;
    let err = parse_manifest(json).unwrap_err();
    assert!(
        format!("{err:#}").contains("unsupported bundle version 2"),
        "{err:#}"
    );
}

#[test]
fn report_errors_when_any_failed() {
    let outcomes = vec![
        EntryOutcome::Fetched(10),
        EntryOutcome::Skipped,
        EntryOutcome::Failed {
            path: "x".into(),
            err: "boom".into(),
        },
    ];
    let err = report(
        &outcomes,
        &[],
        Transfer::default(),
        DedupSummary::default(),
        0,
        Path::new("/out"),
        true,
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("1 entr"), "{err:#}");
}

#[test]
fn report_ok_when_none_failed() {
    let outcomes = vec![EntryOutcome::Fetched(10), EntryOutcome::Skipped];
    assert!(
        report(
            &outcomes,
            &[],
            Transfer::default(),
            DedupSummary::default(),
            0,
            Path::new("/out"),
            false
        )
        .is_ok()
    );
}

/// An entry whose blob differs in size from its manifest succeeds: the
/// report names it on one warning line and the pull still succeeds.
#[test]
fn a_manifest_size_mismatch_warns_and_succeeds() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("big.bin"), b"twelve bytes").expect("write");
    std::fs::write(tmp.path().join("ok.bin"), b"four").expect("write");
    let entries = [
        ManifestEntry {
            path: "big.bin".into(),
            hash: "b3:aa".into(),
            size: Some(4),
            chunks: None,
        },
        ManifestEntry {
            path: "ok.bin".into(),
            hash: "b3:bb".into(),
            size: Some(4),
            chunks: None,
        },
    ];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let outcomes = vec![EntryOutcome::Fetched(12), EntryOutcome::Fetched(4)];
    let updates = build_completed_updates(&refs, &outcomes, tmp.path());
    let warnings = size_warnings(&refs, &updates);
    assert_eq!(
        warnings,
        vec!["big.bin: manifest says 4 bytes, the blob is 12 bytes".to_string()]
    );
    assert!(
        report(
            &outcomes,
            &warnings,
            Transfer::default(),
            DedupSummary::default(),
            0,
            tmp.path(),
            false
        )
        .is_ok(),
        "a size warning is not a failure"
    );
}

/// The summary counts every entry: a whole-file reuse reads `reused`, and
/// the entries the filter or `--select` dropped read `excluded` (#2190).
#[test]
fn the_summary_counts_reused_and_excluded_entries() {
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
    let rep = pull_report(
        &outcomes,
        transfer,
        DedupSummary::default(),
        3,
        Path::new("/out"),
    );
    assert_eq!(
        counts_line(&rep),
        "pulled into /out (1 fetched, 1 linked, 1 skipped, 1 reused, 3 excluded, 1 failed)"
    );
    assert_eq!(rep.reused_bytes, 7);

    let json = serde_json::to_value(&rep).expect("serialize");
    assert_eq!(json["reused"], 1);
    assert_eq!(json["reused_bytes"], 7);
    assert_eq!(json["excluded"], 3);
    assert_eq!(json["resumed_bytes"], 6);
    assert_eq!(json["downloaded"], 4);
    assert!(json.get("deduped").is_none(), "{json}");
}

#[test]
fn human_bytes_scales_units() {
    assert_eq!(human_bytes(0), "0 B");
    assert_eq!(human_bytes(512), "512 B");
    assert_eq!(human_bytes(1_500), "1.5 KB");
    assert_eq!(human_bytes(27_600_000_000), "27.6 GB");
}

#[test]
fn transfer_line_shows_arrow_only_when_they_differ() {
    // Equal → a single figure, no "→ reconstructed" clause.
    assert_eq!(
        transfer_line(Transfer {
            downloaded: 27_600_000_000,
            reconstructed: 27_600_000_000,
            resumed: 0,
        }),
        "downloaded 27.6 GB"
    );
    // Differ (dedup saved bytes) → both figures with the arrow.
    assert_eq!(
        transfer_line(Transfer {
            downloaded: 13_800_000_000,
            reconstructed: 27_600_000_000,
            resumed: 0,
        }),
        "downloaded 13.8 GB → reconstructed 27.6 GB"
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

fn entry(path: &str, hash: &str) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        hash: hash.into(),
        size: None,
        chunks: None,
    }
}

/// The core of #1306: entries naming the same blob collapse into one group, so
/// the fan-out fetches (and pays for) that blob exactly once. First-seen order
/// is preserved both across groups and within a group.
#[test]
fn group_by_hash_collapses_duplicates_preserving_order() {
    let entries = [
        entry("a.txt", "b3:h1"),
        entry("b.txt", "b3:h2"),
        entry("c.txt", "b3:h1"),
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
    let entries = [entry("a", "b3:1"), entry("b", "b3:2"), entry("c", "b3:3")];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let groups = group_by_hash(&refs);
    assert_eq!(groups.len(), 3);
    assert!(groups.iter().all(|group| group.entries.len() == 1));
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

fn filtered_paths(include: &[&str], exclude: &[&str], entries: Vec<ManifestEntry>) -> Vec<String> {
    let filter = EntryFilter::compile(&strs(include), &strs(exclude)).unwrap();
    filter
        .apply_reporting(entries)
        .kept
        .into_iter()
        .map(|e| e.path)
        .collect()
}

fn sample() -> Vec<ManifestEntry> {
    vec![
        entry("models/a.bin", "b3:1"),
        entry("models/b.txt", "b3:2"),
        entry("docs/readme.md", "b3:3"),
        entry("docs/deep/notes.txt", "b3:4"),
    ]
}

/// No flags => every entry passes, in manifest order.
#[test]
fn entry_filter_passthrough_when_no_flags() {
    assert_eq!(
        filtered_paths(&[], &[], sample()),
        vec![
            "models/a.bin",
            "models/b.txt",
            "docs/readme.md",
            "docs/deep/notes.txt"
        ]
    );
}

/// `--include` is a whitelist gate: absent it opens, present an entry must
/// match at least one pattern. `*` does not cross `/`, so `models/*` keeps
/// only the direct children of `models/`.
#[test]
fn entry_filter_include_is_a_whitelist_gate() {
    assert_eq!(
        filtered_paths(&["models/*"], &[], sample()),
        vec!["models/a.bin", "models/b.txt"]
    );
}

/// Multiple `--include` patterns are OR-ed.
#[test]
fn entry_filter_includes_are_ored() {
    assert_eq!(
        filtered_paths(&["models/*.bin", "docs/readme.md"], &[], sample()),
        vec!["models/a.bin", "docs/readme.md"]
    );
}

/// `--exclude` drops matches; multiple patterns are OR-ed. A leading `**/`
/// is what makes a suffix glob match at any depth — a bare `*.txt` matches
/// only a root-level file, since `*` never crosses `/`.
#[test]
fn entry_filter_excludes_are_ored() {
    assert_eq!(
        filtered_paths(&[], &["**/*.txt", "**/*.md"], sample()),
        vec!["models/a.bin"]
    );
}

/// A bare `*.txt` does NOT cross `/`, so it leaves nested `.txt` entries in
/// place — the same gitignore separator rule `origin import --exclude` uses.
#[test]
fn entry_filter_star_does_not_cross_slash() {
    assert_eq!(
        filtered_paths(&[], &["*.txt"], sample()),
        vec![
            "models/a.bin",
            "models/b.txt",
            "docs/readme.md",
            "docs/deep/notes.txt"
        ]
    );
}

/// `--exclude` wins over `--include`: an entry matching both is dropped.
#[test]
fn entry_filter_exclude_beats_include() {
    assert_eq!(
        filtered_paths(&["models/*"], &["**/*.txt"], sample()),
        vec!["models/a.bin"]
    );
}

/// `**` recurses across `/` where a one-level `*` does not.
#[test]
fn entry_filter_double_star_recurses() {
    assert_eq!(
        filtered_paths(&["docs/**"], &[], sample()),
        vec!["docs/readme.md", "docs/deep/notes.txt"]
    );
    assert_eq!(
        filtered_paths(&["docs/*"], &[], sample()),
        vec!["docs/readme.md"]
    );
}

/// A filter that matches nothing yields an empty set (the caller then
/// reports "no entries match"), never an error.
#[test]
fn entry_filter_can_empty_the_set() {
    assert!(filtered_paths(&["no/such/*"], &[], sample()).is_empty());
}

/// Each pattern that matches no entry is reported with its flag, and one
/// whose `**/` form would match gets that form as a hint: every pattern
/// matches from the bundle root (#2190).
#[test]
fn entry_filter_reports_each_pattern_that_matched_nothing() {
    let mut entries = sample();
    entries.push(entry("gpt/metal/model.bin", "b3:5"));
    let filter = EntryFilter::compile(
        &strs(&["models/*", "no/such/*"]),
        &strs(&["metal/*", "**/*.md"]),
    )
    .unwrap();
    let pass = filter.apply_reporting(entries);
    assert_eq!(
        pass.kept
            .iter()
            .map(|e| e.path.as_str())
            .collect::<Vec<_>>(),
        vec!["models/a.bin", "models/b.txt"]
    );
    assert_eq!(pass.excluded, 3);
    assert_eq!(
        pass.unmatched,
        vec![
            Unmatched {
                flag: "--include",
                pattern: "no/such/*".into(),
                suggestion: None,
            },
            Unmatched {
                flag: "--exclude",
                pattern: "metal/*".into(),
                suggestion: Some("**/metal/*".into()),
            },
        ]
    );
    assert_eq!(
        pass.unmatched[0].warning(),
        "--include 'no/such/*' matched no entries"
    );
    assert_eq!(
        pass.unmatched[1].warning(),
        "--exclude 'metal/*' matched no entries (a pattern matches the whole path from the \
         bundle root; did you mean '**/metal/*'?)"
    );
}

/// A pattern with no `/` is anchored too: `*.txt` matches only a root-level
/// file, so a miss suggests `**/*.txt`. A pattern that already starts with
/// `**` gets no hint.
#[test]
fn entry_filter_hints_an_anchored_pattern_without_a_slash() {
    let filter = EntryFilter::compile(&[], &strs(&["*.txt", "**/*.png"])).unwrap();
    let pass = filter.apply_reporting(sample());
    assert_eq!(pass.excluded, 0);
    assert_eq!(
        pass.unmatched
            .iter()
            .map(|u| (u.pattern.as_str(), u.suggestion.as_deref()))
            .collect::<Vec<_>>(),
        vec![("*.txt", Some("**/*.txt")), ("**/*.png", None)]
    );
}

/// Patterns that each match an entry report nothing, even an `--exclude`
/// that matches only entries the include gate already dropped.
#[test]
fn entry_filter_reports_nothing_when_every_pattern_matches() {
    let filter = EntryFilter::compile(&strs(&["models/*"]), &strs(&["docs/**"])).unwrap();
    let pass = filter.apply_reporting(sample());
    assert!(pass.unmatched.is_empty(), "{:?}", pass.unmatched);
    assert_eq!(pass.excluded, 2);

    let pass = EntryFilter::compile(&[], &[])
        .unwrap()
        .apply_reporting(sample());
    assert!(pass.unmatched.is_empty());
    assert_eq!(pass.excluded, 0);
}

/// `--select` adds each entry it drops to the filter's excluded count.
#[test]
fn a_selection_adds_its_dropped_entries_to_the_excluded_count() {
    let kept = Kept {
        manifest: Manifest {
            version: 1,
            entries: sample(),
        },
        excluded: 2,
    };
    let kept = apply_selection(kept, |entries| {
        parse_selection("models/a.bin\ndocs/readme.md\n", entries)
    })
    .unwrap();
    assert_eq!(kept.manifest.entries.len(), 2);
    assert_eq!(kept.excluded, 4);
}

/// The run-end scan counts the staging files of blobs the run did not
/// use, once per blob, totals the disk space they take, and ignores the
/// run's own blobs, the manifest blob, and names that are not a blob's
/// staging file (#2190).
#[test]
fn leftover_partials_counts_only_unselected_blobs() {
    let tmp = tempfile::tempdir().expect("tmp");
    assert_eq!(
        leftover_partials(tmp.path(), &HashSet::new()),
        Leftovers::default(),
        "no staging dir, nothing left over"
    );

    let dir = tmp.path().join(STAGING_DIR);
    std::fs::create_dir_all(&dir).expect("staging dir");
    let hex = |b: u8| blake3::Hash::from_bytes([b; 32]).to_hex().to_string();
    let write = |name: String, len: usize| {
        std::fs::write(dir.join(name), vec![0u8; len]).expect("write");
    };
    // The run's selected entry and the manifest blob: kept for this run.
    write(format!("{}.partial", hex(1)), 500);
    write(format!("{}.partial.ranges", hex(1)), 5);
    write(format!("{}.partial", hex(2)), 300);
    // An orphan partial and its record, and a finalized orphan blob.
    write(format!("{}.partial", hex(3)), 1000);
    write(format!("{}.partial.ranges", hex(3)), 10);
    write(hex(4), 200);
    // A record's temporary file and a stray name.
    write(".tmpAbC123".into(), 50);
    write("notes.txt".into(), 50);

    let keep: HashSet<[u8; 32]> = [[1; 32], [2; 32]].into_iter().collect();
    let on_disk =
        |name: String| allocated_bytes(&std::fs::metadata(dir.join(name)).expect("metadata"));
    let expected = on_disk(format!("{}.partial", hex(3)))
        + on_disk(format!("{}.partial.ranges", hex(3)))
        + on_disk(hex(4));
    let leftovers = leftover_partials(tmp.path(), &keep);
    assert_eq!(
        leftovers,
        Leftovers {
            blobs: 2,
            bytes: expected,
        }
    );
    assert_eq!(
        leftover_warning(
            Path::new("/out/.decdn-partial"),
            &Leftovers {
                blobs: 2,
                bytes: 1210,
            }
        )
        .as_deref(),
        Some(
            "2 partial downloads (1.2 KB on disk) that this run did not use remain in \
             /out/.decdn-partial; delete them to reclaim the space"
        )
    );
    assert_eq!(
        leftover_warning(
            Path::new("/out/.decdn-partial"),
            &Leftovers {
                blobs: 1,
                bytes: 1_500_000_000,
            }
        )
        .as_deref(),
        Some(
            "1 partial download (1.5 GB on disk) that this run did not use remains in \
             /out/.decdn-partial; delete it to reclaim the space"
        )
    );
    assert_eq!(
        leftover_warning(Path::new("/out"), &Leftovers::default()),
        None
    );
}

/// A range-dedup `.partial` sized to the whole blob before its bytes land
/// counts only the space it takes, not its length (#2190).
#[cfg(unix)]
#[test]
fn leftover_partials_count_a_sparse_partial_by_its_allocated_space() {
    let tmp = tempfile::tempdir().expect("tmp");
    let dir = tmp.path().join(STAGING_DIR);
    std::fs::create_dir_all(&dir).expect("staging dir");
    let hex = blake3::Hash::from_bytes([9; 32]).to_hex().to_string();
    let file = std::fs::File::create(dir.join(format!("{hex}.partial"))).expect("create");
    file.set_len(64 * 1024 * 1024).expect("size the partial");
    let leftovers = leftover_partials(tmp.path(), &HashSet::new());
    assert_eq!(leftovers.blobs, 1);
    assert!(
        leftovers.bytes < 1024 * 1024,
        "a hole-only partial takes almost no space: {}",
        leftovers.bytes
    );
}

/// A malformed glob is a hard error naming the flag it came from.
#[test]
fn entry_filter_rejects_bad_glob() {
    let err = EntryFilter::compile(&strs(&["["]), &[]).unwrap_err();
    assert!(
        format!("{err:#}").contains("--include"),
        "error should name the offending flag: {err:#}"
    );
}

/// A duplicate destination is materialized from the canonical file (no second
/// paid fetch), lands with identical bytes, creates any missing parent dir,
/// and leaves the source in place (it is a link/copy, not a move).
#[test]
fn link_or_copy_atomic_materializes_identical_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("nested/dest.bin");
    std::fs::write(&src, b"the-canonical-bytes").unwrap();

    link_or_copy_atomic(&src, &dest).unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), b"the-canonical-bytes");
    assert!(src.exists());
}

/// Materialization atomically *replaces* an existing destination, upholding the
/// "a present final file is verified-good" invariant skip-existing relies on.
#[test]
fn link_or_copy_atomic_replaces_existing_dest() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("dest.bin");
    std::fs::write(&src, b"new").unwrap();
    std::fs::write(&dest, b"stale-and-longer").unwrap();

    link_or_copy_atomic(&src, &dest).unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), b"new");
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
    let entries = [entry("present.txt", "b3:h"), entry("absent.txt", "b3:h")];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let skip = HashSet::from(["present.txt".to_string()]);

    let slots = plan_slots(&refs, out, false, &skip);
    assert!(matches!(slots[0], Slot::Skip));
    assert!(matches!(slots[1], Slot::Write { .. }));

    let slots = plan_slots(&refs, out, true, &skip);
    assert!(matches!(slots[0], Slot::Write { .. }));
    assert!(matches!(slots[1], Slot::Write { .. }));
}

/// A path that escapes `out_root` is a per-destination failure, not a fetch.
#[test]
fn plan_slots_marks_unsafe_paths_failed() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [entry("../escape", "b3:h")];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let slots = plan_slots(&refs, dir.path(), false, &HashSet::new());
    assert!(matches!(slots[0], Slot::Failed(_)));
}

/// A manifest path inside the reserved staging dir must not be materialized —
/// otherwise it could collide with a per-hash staging file and `remove_staging`
/// could delete a real output.
#[test]
fn plan_slots_rejects_the_reserved_staging_dir() {
    let dir = tempfile::tempdir().unwrap();
    let entries = [entry(&format!("{STAGING_DIR}/deadbeef.partial"), "b3:h")];
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let slots = plan_slots(&refs, dir.path(), false, &HashSet::new());
    assert!(matches!(slots[0], Slot::Failed(_)));
}

/// An unrecorded (empty saved manifest) file still skips via the re-hash gate
/// when its on-disk bytes already match the new manifest hash.
#[tokio::test]
async fn resolve_disk_state_skips_matching_file_by_rehash() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = b"hello world";
    std::fs::write(tmp.path().join("a.txt"), body).expect("write");
    let h = format!("b3:{}", blake3::hash(body).to_hex());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: h,
        size: Some(u64::try_from(body.len()).expect("len")),
        chunks: None,
    }];
    // Empty saved manifest → falls to the re-hash gate, still skips (content matches).
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(st.skip.contains("a.txt"));
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
    let claimed = "b3:0000000000000000000000000000000000000000000000000000000000000000".to_string();
    // Document the assumption the skip proof rests on: the planted bytes do
    // not hash to `claimed`, so a re-hash would fetch and only a
    // record-trusting fast-skip can pass.
    assert_ne!(
        claimed,
        format!("b3:{}", blake3::hash(body).to_hex()),
        "planted bytes must not match the claimed hash"
    );
    let mut updates = BTreeMap::new();
    updates.insert(
        "a.txt".to_string(),
        bundle_manifest::SavedFile {
            hash: claimed.clone(),
            size,
            mtime,
            chunks: None,
        },
    );
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = bundle_manifest::load(tmp.path());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: claimed, // equals the saved record's hash → fast-skip candidate
        size: Some(size),
        chunks: None,
    }];
    let st = resolve_disk_state(&entries, &saved, tmp.path(), false).await;
    // Skipped on the record alone; a re-hash of the (non-matching) bytes would fetch.
    assert!(st.skip.contains("a.txt"));
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
    let hash = "b3:1111111111111111111111111111111111111111111111111111111111111111".to_string();
    let mut updates = BTreeMap::new();
    updates.insert(
        "a.bin".to_string(),
        bundle_manifest::SavedFile {
            hash: hash.clone(),
            size,
            mtime: SavedMtime::of(&meta).expect("mtime"),
            chunks: None,
        },
    );
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = bundle_manifest::load(tmp.path());
    let entries = [ManifestEntry {
        path: "a.bin".into(),
        hash,
        size: Some(size - 7),
        chunks: None,
    }];
    let st = resolve_disk_state(&entries, &saved, tmp.path(), false).await;
    assert!(st.skip.contains("a.bin"), "accepted by its hash");
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
    let real = format!("b3:{}", blake3::hash(body).to_hex());
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
        bundle_manifest::SavedFile {
            hash: real.clone(),
            size,
            mtime: stale,
            chunks: None,
        },
    );
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = bundle_manifest::load(tmp.path());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: real,
        size: Some(size),
        chunks: None,
    }];
    let st = resolve_disk_state(&entries, &saved, tmp.path(), false).await;
    // Re-hash confirmed the content is identical → skip, no re-fetch.
    assert!(st.skip.contains("a.txt"));
}

/// A changed file (content no longer matches the new manifest hash) is never
/// skipped, whether or not a saved record exists for it.
#[tokio::test]
async fn resolve_disk_state_fetches_changed_file() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join("a.txt"), b"OLD CONTENT").expect("write");
    let new = format!("b3:{}", blake3::hash(b"NEW CONTENT").to_hex());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: new,
        size: Some(11),
        chunks: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains("a.txt")); // hash mismatch → fetch
}

/// An absent file always fetches — nothing to re-hash, no fast-skip possible.
#[tokio::test]
async fn resolve_disk_state_absent_file_fetches() {
    let tmp = tempfile::tempdir().expect("tmp");
    let entries = [ManifestEntry {
        path: "missing.txt".into(),
        hash: "b3:00".into(),
        size: Some(1),
        chunks: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains("missing.txt"));
}

/// `--overwrite` bypasses the pre-pass entirely: nothing is ever skipped.
#[tokio::test]
async fn resolve_disk_state_overwrite_skips_nothing() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = b"hello world";
    std::fs::write(tmp.path().join("a.txt"), body).expect("write");
    let h = format!("b3:{}", blake3::hash(body).to_hex());
    let entries = [ManifestEntry {
        path: "a.txt".into(),
        hash: h,
        size: Some(u64::try_from(body.len()).expect("len")),
        chunks: None,
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
    let h = format!("b3:{}", blake3::hash(&body).to_hex());
    // Prior run recorded game1/lib/dup.dll.
    let old_entries = [ManifestEntry {
        path: "game1/lib/dup.dll".into(),
        hash: h.clone(),
        size: Some(4096),
        chunks: None,
    }];
    let updates = build_completed_updates(
        &old_entries.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(4096)],
        tmp.path(),
    );
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved");
    let saved = bundle_manifest::load(tmp.path());
    // The NEW bundle wants the same content at a different path.
    let new_entries = vec![ManifestEntry {
        path: "game2/lib/dup.dll".into(),
        hash: h.clone(),
        size: Some(4096),
        chunks: None,
    }];
    let st = resolve_disk_state(&new_entries, &saved, tmp.path(), false).await;
    let want = fetch::parse_hash(&h).expect("hash");
    assert_eq!(
        st.whole_file.get(&want),
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
    let old_hash = format!("b3:{}", blake3::hash(&old_body).to_hex());
    let old_entries = [ManifestEntry {
        path: "changed.bin".into(),
        hash: old_hash.clone(),
        size: Some(32),
        chunks: None,
    }];
    let updates = build_completed_updates(
        &old_entries.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(32)],
        tmp.path(),
    );
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved");
    let saved = bundle_manifest::load(tmp.path());

    // The new manifest wants DIFFERENT content at the SAME path: this entry
    // is in scope and will be written (not skipped), so its saved record's
    // hash must not become a whole-file donor.
    let new_hash = format!("b3:{}", blake3::hash(&[1u8; 32]).to_hex());
    let new_entries = vec![ManifestEntry {
        path: "changed.bin".into(),
        hash: new_hash,
        size: Some(32),
        chunks: None,
    }];

    let st = resolve_disk_state(&new_entries, &saved, tmp.path(), false).await;
    assert!(
        !st.skip.contains("changed.bin"),
        "changed content must fetch"
    );
    let old_want = fetch::parse_hash(&old_hash).expect("hash");
    assert_eq!(
        st.whole_file.get(&old_want),
        None,
        "a will-write path must never be indexed as a whole-file donor"
    );
}

/// `overwrite` builds no index (nothing is reused).
#[tokio::test]
async fn resolve_disk_state_overwrite_builds_no_whole_file_index() {
    let tmp = tempfile::tempdir().expect("tmp");
    let body = vec![7u8; 16];
    std::fs::write(tmp.path().join("a.bin"), &body).expect("write");
    let h = format!("b3:{}", blake3::hash(&body).to_hex());
    let old = [ManifestEntry {
        path: "a.bin".into(),
        hash: h.clone(),
        size: Some(16),
        chunks: None,
    }];
    let updates = build_completed_updates(
        &old.iter().collect::<Vec<_>>(),
        &[EntryOutcome::Fetched(16)],
        tmp.path(),
    );
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates).expect("w");
    let saved = bundle_manifest::load(tmp.path());
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
    let h = format!("b3:{}", blake3::hash(body).to_hex());
    let entries = [ManifestEntry {
        path: "link.txt".into(),
        hash: h,
        size: Some(u64::try_from(body.len()).expect("len")),
        chunks: None,
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(!st.skip.contains("link.txt"));
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
    let uh = format!("b3:{}", blake3::hash(&body).to_hex());
    let ch = format!("b3:{}", blake3::hash(&body).to_hex()); // single-chunk == whole file
    let entries = [ManifestEntry {
        path: "u.bin".into(),
        hash: uh,
        size: Some(20),
        chunks: Some(vec![ManifestChunk { hash: ch, size: 20 }]),
    }];
    let st = resolve_disk_state(&entries, &SavedManifest::default(), tmp.path(), false).await;
    assert!(st.skip.contains("u.bin"));
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
    let old_hash = format!("b3:{}", blake3::hash(&old_body).to_hex());
    let old_chunk_hash = old_hash.clone(); // single-chunk == whole file
    let old_entries = [ManifestEntry {
        path: "c.bin".into(),
        hash: old_hash,
        size: Some(20),
        chunks: Some(vec![ManifestChunk {
            hash: old_chunk_hash.clone(),
            size: 20,
        }]),
    }];
    let old_outcomes = vec![EntryOutcome::Fetched(20)];
    let old_refs: Vec<&ManifestEntry> = old_entries.iter().collect();
    let updates = build_completed_updates(&old_refs, &old_outcomes, tmp.path());
    bundle_manifest::merge_and_write(tmp.path(), SavedManifest::default(), updates)
        .expect("write saved manifest");
    let saved = bundle_manifest::load(tmp.path());

    // The new manifest declares different content, but the OLD file is
    // still on disk (not yet overwritten) — a fetch, not a skip, and the
    // old bytes remain a valid donor until this entry's own materialize.
    let new_hash = format!("b3:{}", blake3::hash(&[2u8; 20]).to_hex());
    let new_entries = vec![ManifestEntry {
        path: "c.bin".into(),
        hash: new_hash,
        size: Some(20),
        chunks: None,
    }];

    let st = resolve_disk_state(&new_entries, &saved, tmp.path(), false).await;
    assert!(!st.skip.contains("c.bin"), "changed content must fetch");
    // The per-entry "changed" branch and the whole-root records pass both
    // seed this same donor (harmless duplication, see resolve_disk_state's
    // doc comment) — assert every seed entry present matches, rather than
    // pinning an exact count.
    assert!(!st.seed.is_empty());
    let want_hash = fetch::parse_hash(&old_chunk_hash).expect("parse old chunk hash");
    for donor in &st.seed {
        assert_eq!(donor.hash, want_hash);
        assert_eq!(donor.source, path);
        assert_eq!(donor.offset, 0);
        assert_eq!(donor.len, 20);
    }
}

/// `materialize` (the paid-path writer) atomically replaces an existing
/// destination from the staging file, and leaves the staging file intact so a
/// retry for the next duplicate path can read it again.
#[test]
fn materialize_replaces_dest_and_keeps_staging() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("blob.partial");
    std::fs::write(&staging, b"new-content").unwrap();
    let dest = dir.path().join("out.bin");
    std::fs::write(&dest, b"stale-old").unwrap();

    let n = materialize(&staging, &dest).unwrap();

    assert_eq!(n, b"new-content".len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), b"new-content");
    assert!(staging.exists(), "staging must survive for retries");
    assert_eq!(std::fs::read(&staging).unwrap(), b"new-content");
}

/// A non-donor blob's first write moves staging into place — no second copy
/// of the blob — and replaces a stale destination atomically.
#[test]
fn first_write_moves_a_non_donor_blob_into_place() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("blob");
    std::fs::write(&staging, b"new-content").unwrap();
    let dest = dir.path().join("sub").join("out.bin");
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::write(&dest, b"stale-old").unwrap();

    let n = first_write(&staging, &dest, false).unwrap();

    assert_eq!(n, b"new-content".len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), b"new-content");
    assert!(!staging.exists(), "the blob moved, it was not copied");
}

/// A donor blob's first write keeps its staging name — later recipients
/// still splice from it — and shares its storage with the destination.
#[cfg(unix)]
#[test]
fn first_write_links_a_donor_blob_and_keeps_staging() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("blob");
    std::fs::write(&staging, b"donor-bytes").unwrap();
    let dest = dir.path().join("out.bin");

    let n = first_write(&staging, &dest, true).unwrap();

    assert_eq!(n, b"donor-bytes".len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), b"donor-bytes");
    assert!(staging.exists(), "a donor's staging name survives");
    assert_eq!(
        std::fs::metadata(&staging).unwrap().ino(),
        std::fs::metadata(&dest).unwrap().ino(),
        "the first destination is a hard link, not a copy"
    );
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
    let outcomes = materialize_from_donor(slots, &donor, 2048).await;
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
    let outcomes = materialize_from_donor(slots, &donor, 10).await;
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
    let outcomes = materialize_from_donor(slots, &donor, 5).await;
    assert!(matches!(outcomes[0], EntryOutcome::Failed { .. }));
    assert!(matches!(outcomes[1], EntryOutcome::Deduped(5)));
}

/// A linked duplicate is counted separately and never fails the pull.
#[test]
fn report_counts_linked_without_failing() {
    let outcomes = vec![
        EntryOutcome::Fetched(10),
        EntryOutcome::Linked,
        EntryOutcome::Skipped,
    ];
    assert!(
        report(
            &outcomes,
            &[],
            Transfer::default(),
            DedupSummary::default(),
            0,
            Path::new("/out"),
            false
        )
        .is_ok()
    );
}

/// `--namespace` on bundle pull is bundle-level: one id for the whole run. It
/// rejects the reserved `0` (the `NO_NAMESPACE` sentinel — omit the flag instead),
/// reusing the same parser as `decdn fetch`.
#[test]
fn bundle_pull_namespace_flag_parses_and_rejects_zero() {
    use clap::Parser;
    #[derive(Parser)]
    struct T {
        #[command(flatten)]
        a: decdn_common::cli::BundlePullArgs,
    }
    let ok = T::try_parse_from(["t", "-o", "out", "--hash", "b3:aa", "--namespace", "7"])
        .expect("valid namespace parses");
    assert_eq!(ok.a.namespace, Some(7));

    assert!(
        T::try_parse_from(["t", "-o", "out", "--hash", "b3:aa", "--namespace", "0"]).is_err(),
        "namespace 0 is the reserved sentinel and must be rejected"
    );

    let none =
        T::try_parse_from(["t", "-o", "out", "--hash", "b3:aa"]).expect("namespace is optional");
    assert_eq!(none.a.namespace, None, "absent flag stays None");
}

/// `--dry-run` must still enforce the flag-combination rule: a dangling
/// `--provider-address` (no `--node-id`) is rejected even for `--dry-run`,
/// which does not run at parse time — so `bundle_pull` calls `validate()`
/// BEFORE the dry-run short-circuit. Asserted end-to-end: the command
/// returns the guard error (before any network/chain I/O, since
/// `validate()` fails first).
#[tokio::test]
async fn dry_run_still_rejects_a_dangling_provider_address() {
    use clap::Parser;
    #[derive(Parser)]
    struct T {
        #[command(flatten)]
        a: decdn_common::cli::BundlePullArgs,
    }
    let addr = "0x0000000000000000000000000000000000000001";
    let args = T::parse_from([
        "t",
        "-o",
        "out",
        "--hash",
        "b3:aa",
        "--dry-run",
        "--provider-address",
        addr,
    ])
    .a;
    let err = super::bundle_pull(&args, None)
        .await
        .expect_err("a dangling --provider-address must be rejected even for --dry-run");
    assert!(
        format!("{err:#}").contains("--provider-address requires --node-id"),
        "expected the validate() guard error, got: {err:#}"
    );
}

/// Parse `bundle pull` args from `extra`, behind a placeholder program name.
fn pull_args(extra: &[&str]) -> BundlePullArgs {
    use clap::Parser;
    #[derive(Parser)]
    struct T {
        #[command(flatten)]
        a: decdn_common::cli::BundlePullArgs,
    }
    T::parse_from(std::iter::once("t").chain(extra.iter().copied())).a
}

/// Cache `manifest` under `out` as `bundle pull --hash` caches it, and return
/// the bundle's `b3:` hash.
fn cache_manifest(out: &Path, manifest: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(manifest).unwrap();
    let hash = *blake3::hash(&bytes).as_bytes();
    bundle_cache::store(out, hash, &bytes);
    format!("b3:{}", blake3::Hash::from_bytes(hash).to_hex())
}

/// `--hash --dry-run` reads the bundle's cached manifest, even under
/// `--overwrite`, so the filters run against its entries (#2327).
#[test]
fn dry_run_reads_a_cached_hash_manifest() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let hash = cache_manifest(
        tmp.path(),
        &serde_json::json!({
            "version": 1,
            "entries": [
                {"path": "openai-gpt-oss-20b/config.json", "hash": "b3:aa", "size": 10},
                {"path": "openai-gpt-oss-20b/metal/model.bin", "hash": "b3:bb", "size": 20},
            ],
        }),
    );
    for overwrite in [false, true] {
        let mut argv = vec!["-o", out, "--hash", &hash, "--dry-run"];
        if overwrite {
            argv.push("--overwrite");
        }
        let manifest = dry_run_manifest(&pull_args(&argv))
            .unwrap()
            .expect("the cached manifest is read");
        assert_eq!(
            manifest
                .entries
                .iter()
                .map(|e| e.path.as_str())
                .collect::<Vec<_>>(),
            vec![
                "openai-gpt-oss-20b/config.json",
                "openai-gpt-oss-20b/metal/model.bin"
            ],
            "overwrite={overwrite}"
        );
    }
}

/// `--hash --dry-run` with no cached manifest has no entries to list, and the
/// full dry run still finishes with no endpoint, chain, or keystore.
#[tokio::test]
async fn dry_run_has_no_manifest_for_an_uncached_hash() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let hash = format!("b3:{}", "ab".repeat(32));
    let args = pull_args(&["-o", out, "--hash", &hash, "--dry-run"]);
    assert!(dry_run_manifest(&args).unwrap().is_none());
    super::bundle_pull(&args, None).await.unwrap();
}

/// The full `bundle_pull --dry-run` reads the cached `--hash` manifest: a
/// cached blob that hashes to the bundle but is not a v1 manifest fails the
/// dry run, naming the cache file, as it would fail a real pull.
#[tokio::test]
async fn dry_run_fails_on_a_cached_manifest_that_does_not_parse() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let hash = cache_manifest(
        tmp.path(),
        &serde_json::json!({"version": 2, "entries": []}),
    );
    let args = pull_args(&["-o", out, "--hash", &hash, "--dry-run"]);
    let err = super::bundle_pull(&args, None)
        .await
        .expect_err("a cached non-v1 manifest fails the dry run");
    let msg = format!("{err:#}");
    assert!(msg.contains("unsupported bundle version 2"), "{msg}");
    assert!(msg.contains(bundle_cache::CACHE_DIR), "{msg}");
}

/// A malformed `--hash` fails the dry run, as it fails a real pull.
#[tokio::test]
async fn dry_run_rejects_a_malformed_hash() {
    let tmp = tempfile::tempdir().expect("tmp");
    let out = tmp.path().to_str().expect("utf-8 tmp path");
    let args = pull_args(&["-o", out, "--hash", "b3:aa", "--dry-run"]);
    let err = super::bundle_pull(&args, None)
        .await
        .expect_err("a malformed --hash fails the dry run");
    assert!(format!("{err:#}").contains("invalid hash"), "{err:#}");
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
        bundle_manifest::SavedFile {
            hash: "b3:00".into(),
            size: 3,
            mtime: SavedMtime { secs: 1, nanos: 0 },
            chunks: None,
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

/// A lane's widen hooks grant only the provider's permits that are free
/// now, beside the lane's own lease, and give each one back. At a cap of
/// 3 or more, extra streams leave the last free permit for a sibling
/// entry's first stream; a restart takes it (#2252).
#[tokio::test]
async fn a_lane_widens_only_into_free_permits_and_gives_them_back() {
    use decdn_client::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(3);
    let lease = cap.try_permit(p1).await.expect("the lane's own permit");
    let extras = cap.extra_permits(p1).await;
    assert!(extras.grant(Extra), "one extra stream");
    assert!(!extras.grant(Extra), "the last free permit is kept back");
    assert!(extras.grant(Restart), "a restart takes the last permit");
    assert!(!extras.grant(Restart), "the cap is full");
    extras.release_one();
    assert!(extras.grant(Restart), "a released permit is free again");
    extras.release_one();
    extras.release_one();
    drop(lease);
    let all: Vec<_> = futures_util::future::join_all((0..3).map(|_| cap.try_permit(p1)))
        .await
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(all.len(), 3, "every permit came back");
}

/// At a cap of 2 the lease and one extra stream fill the cap, so an extra
/// stream takes the last free permit: keeping it back would leave a lane
/// no extra stream at all.
#[tokio::test]
async fn a_cap_of_two_still_grants_an_extra_stream() {
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(2);
    let _lease = cap.try_permit(p1).await.expect("the lane's own permit");
    let extras = cap.extra_permits(p1).await;
    assert!(
        extras.grant(decdn_client::GrowFor::Extra),
        "the extra stream"
    );
    assert!(cap.try_permit(p1).await.is_none(), "the cap is full");
    extras.release_one();
}

/// A lane that `grow` refused claims the provider's next free permit. A
/// sibling lane that still runs an extra stream cannot take it, by
/// restart or by extra stream, before the claimant's next ask (#2341).
#[tokio::test]
async fn a_lane_running_a_stream_cannot_restart_into_a_claimed_permit() {
    use decdn_client::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(4);
    let _b_lease = cap.try_permit(p1).await.expect("lane B's own permit");
    let a_lease = cap.try_permit(p1).await.expect("lane A's own permit");
    let a = cap.extra_permits(p1).await;
    let b = cap.extra_permits(p1).await;
    assert!(a.grant(Extra), "lane A's extra stream");
    let c_lease = cap.try_permit(p1).await.expect("lane C's own permit");
    assert!(!b.grant(Extra), "the cap is full: lane B claims");
    drop(a_lease);
    assert!(
        !a.grant(Restart),
        "lane A, still on its extra stream, waits behind lane B's claim"
    );
    assert!(!a.grant(Extra), "an extra stream waits too");
    drop(c_lease);
    assert!(
        !a.grant(Restart),
        "lane B's claim and the kept permit hold both free permits"
    );
    assert!(b.grant(Extra), "lane B takes the permit it claimed");
    assert!(
        a.grant(Restart),
        "with lane B served, lane A's own claim comes first"
    );
    a.release_one();
    a.release_one();
    b.release_one();
}

/// A lane that runs no stream starts again ahead of a claim for one more
/// stream: the claimant runs one, and the free permit cannot serve it
/// yet.
#[tokio::test]
async fn a_lane_that_runs_no_stream_goes_ahead_of_a_claim_for_one_more() {
    use decdn_client::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(4);
    let _b_lease = cap.try_permit(p1).await.expect("lane B's own permit");
    let b = cap.extra_permits(p1).await;
    assert!(b.grant(Extra), "lane B's first extra stream");
    assert!(b.grant(Extra), "lane B's second extra stream");
    assert!(
        !b.grant(Extra),
        "the last free permit is kept: lane B claims"
    );
    let a = cap.extra_permits(p1).await;
    assert!(
        a.grant(Restart),
        "lane A, which gave its lease back, starts again on the free permit"
    );
    a.release_one();
    b.release_one();
    b.release_one();
}

/// A lane's claim ends with the lane, and the other claims stay.
#[tokio::test]
async fn an_ended_lane_claims_nothing() {
    use decdn_client::GrowFor::{Extra, Restart};
    let p1 = Address::repeat_byte(1);
    let cap = LaneStreamCap::new(5);
    let a_lease = cap.try_permit(p1).await.expect("lane A's own permit");
    let _b_lease = cap.try_permit(p1).await.expect("lane B's own permit");
    let _c_lease = cap.try_permit(p1).await.expect("lane C's own permit");
    let a = cap.extra_permits(p1).await;
    let b = cap.extra_permits(p1).await;
    let c = cap.extra_permits(p1).await;
    assert!(a.grant(Extra), "lane A's extra stream");
    assert!(!b.grant(Extra), "lane B claims");
    assert!(!c.grant(Extra), "lane C claims");
    drop(a_lease);
    assert!(
        !a.grant(Restart),
        "two claims and the kept permit hold three"
    );
    drop(b);
    assert!(
        !a.grant(Restart),
        "lane B ended, and lane C's claim still holds a permit"
    );
    drop(c);
    assert!(a.grant(Restart), "no claim is left ahead of lane A");
    a.release_one();
    a.release_one();
}

/// `n` permits of `streams`, held.
fn fill(streams: &ProviderStreams, n: usize) -> Vec<tokio::sync::OwnedSemaphorePermit> {
    (0..n)
        .map(|_| {
            Arc::clone(&streams.semaphore)
                .try_acquire_owned()
                .expect("a free permit")
        })
        .collect()
}

/// Each claim ahead of an ask holds back one free permit, and the
/// claims are served in the order they were made.
#[test]
fn each_claim_ahead_holds_back_one_permit() {
    let (b, c, d) = (1, 2, 3);
    let streams = ProviderStreams::new(4);
    let mut held = fill(&streams, 4);
    assert!(streams.take(b, 0, false).is_none(), "lane B claims first");
    assert!(streams.take(c, 0, false).is_none(), "lane C claims second");
    held.truncate(2);
    assert!(
        streams.take(d, 0, false).is_none(),
        "two claims ahead hold both free permits"
    );
    let mut reheld = fill(&streams, 1);
    assert!(
        streams.take(c, 0, false).is_none(),
        "lane C waits behind lane B"
    );
    assert!(
        streams.take(b, 0, false).is_some(),
        "lane B is served first"
    );
    reheld.clear();
    assert!(streams.take(c, 0, false).is_some(), "lane C is served next");
}

/// The claims ahead hold back their permits and the largest `keep`
/// among them once, not each claim's `keep`: a later ask takes a permit
/// the claims ahead do not need.
#[test]
fn a_later_ask_takes_only_what_the_claims_ahead_leave() {
    let (b, c, d) = (1, 2, 3);
    let streams = ProviderStreams::new(5);
    let mut held = fill(&streams, 5);
    assert!(streams.take(b, 1, false).is_none(), "lane B claims");
    assert!(streams.take(c, 1, false).is_none(), "lane C claims");
    held.truncate(1);
    assert!(
        streams.take(d, 0, false).is_some(),
        "two claims and one kept permit leave a fourth free permit"
    );
    assert!(
        streams.take(b, 1, false).is_some(),
        "lane B is still served"
    );
    assert!(streams.take(c, 1, false).is_some(), "and lane C after it");
}

/// A lane that asks again keeps its place in the claims.
#[test]
fn a_renewed_claim_keeps_its_place() {
    let (b, c) = (1, 2);
    let streams = ProviderStreams::new(2);
    let mut held = fill(&streams, 2);
    assert!(streams.take(b, 0, false).is_none(), "lane B claims first");
    assert!(streams.take(c, 0, false).is_none(), "lane C claims second");
    assert!(streams.take(b, 0, false).is_none(), "lane B asks again");
    held.truncate(1);
    assert!(
        streams.take(c, 0, false).is_none(),
        "lane C still waits behind lane B"
    );
    assert!(
        streams.take(b, 0, false).is_some(),
        "lane B is served first"
    );
}

/// A renewed claim holds back the `keep` of the lane's last ask.
#[test]
fn a_renewed_claim_takes_its_last_asks_keep() {
    let (b, d) = (1, 2);
    for (first, last, granted) in [(1, 0, true), (0, 1, false)] {
        let streams = ProviderStreams::new(3);
        let mut held = fill(&streams, 3);
        assert!(streams.take(b, first, false).is_none(), "lane B claims");
        assert!(streams.take(b, last, false).is_none(), "lane B asks again");
        held.truncate(1);
        assert_eq!(
            streams.take(d, 0, false).is_some(),
            granted,
            "lane B's claim keeps {last} after its own permit"
        );
    }
}

/// An ask from a lane that runs no stream waits only behind the claims
/// of such lanes, in their order.
#[test]
fn a_bare_ask_waits_only_behind_bare_claims() {
    let (b, x, y) = (1, 2, 3);
    let streams = ProviderStreams::new(3);
    let mut held = fill(&streams, 3);
    assert!(
        streams.take(b, 1, false).is_none(),
        "lane B claims one more"
    );
    assert!(
        streams.take(x, 0, true).is_none(),
        "lane X, with none, claims"
    );
    held.truncate(2);
    assert!(
        streams.take(y, 0, true).is_none(),
        "lane Y, with none, waits behind lane X"
    );
    assert!(
        streams.take(x, 0, true).is_some(),
        "lane X goes ahead of lane B's claim"
    );
}

/// A lane's own claim never holds a permit back from the lane itself.
#[test]
fn a_lane_is_not_held_back_by_its_own_claim() {
    let a = 1;
    let streams = ProviderStreams::new(2);
    let _held = fill(&streams, 1);
    assert!(
        streams.take(a, 1, false).is_none(),
        "the kept permit is not an extra stream's"
    );
    assert!(
        streams.take(a, 0, false).is_some(),
        "lane A's restart takes it past its own claim"
    );
}

/// A claim that is not renewed lapses after [`CLAIM_TTL`]; each ask
/// renews it.
#[tokio::test(start_paused = true)]
async fn a_claim_lapses_when_its_lane_stops_asking() {
    let (a, b) = (1, 2);
    let streams = ProviderStreams::new(2);
    let mut held = fill(&streams, 2);
    assert!(streams.take(b, 1, false).is_none(), "lane B claims");
    held.truncate(1);
    assert!(
        streams.take(a, 0, false).is_none(),
        "lane B's claim holds the free permit"
    );
    tokio::time::advance(CLAIM_TTL / 2).await;
    assert!(
        streams.take(b, 1, false).is_none(),
        "lane B asks again and renews its claim"
    );
    tokio::time::advance(CLAIM_TTL / 2 + std::time::Duration::from_millis(1)).await;
    assert!(
        streams.take(a, 0, false).is_none(),
        "a renewed claim holds past the first ask's lapse"
    );
    tokio::time::advance(CLAIM_TTL).await;
    assert!(
        streams.take(a, 0, false).is_some(),
        "a claim not renewed lapses"
    );
}

#[tokio::test]
async fn lane_stream_cap_serializes_one_provider_and_frees_the_rest() {
    let p1 = Address::repeat_byte(1);
    let p2 = Address::repeat_byte(2);

    // n == 1: while the first permit for P1 is held, no second one is free.
    let cap = LaneStreamCap::new(1);
    let held = cap.try_permit(p1).await.unwrap();
    assert!(
        cap.try_permit(p1).await.is_none(),
        "a second same-provider permit is taken while the first is held"
    );
    // A different provider never contends.
    assert!(
        cap.try_permit(p2).await.is_some(),
        "a distinct provider does not share P1's permit"
    );
    // Dropping the first frees it.
    drop(held);
    assert!(
        cap.try_permit(p1).await.is_some(),
        "the permit is free once the first drops"
    );

    // n == 2: two permits for the same provider coexist.
    let cap2 = LaneStreamCap::new(2);
    let a = cap2.try_permit(p1).await.unwrap();
    let b = cap2
        .try_permit(p1)
        .await
        .expect("two same-provider permits coexist at n == 2");
    drop((a, b));
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
) -> anyhow::Result<decdn_client::StreamCandidate<decdn_client::source::ScriptedSource>> {
    let ledger = Arc::new(decdn_client::PoolLedger::new(
        decdn_client::Cumulative::default(),
    ));
    let mut source =
        decdn_client::source::ScriptedSource::new(blob.to_vec())?.paying(Arc::clone(&ledger));
    if let Some(fired) = blip {
        source = source.fault_once_after(256 * 1024, move || {
            fired.store(true, std::sync::atomic::Ordering::SeqCst);
            anyhow!("connection reset")
        });
    }
    Ok(decdn_client::StreamCandidate::new(
        source,
        Arc::new(std::sync::Mutex::new(decdn_client::PoolContext {
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
    decdn_client::StaticSources<decdn_client::source::ScriptedSource>,
    Vec<decdn_client::Holder>,
)> {
    let sources = decdn_client::StaticSources::new(vec![
        scripted_holder(blob, 0xA1, first_blip)?,
        scripted_holder(blob, 0xB2, None)?,
    ])?;
    let holders = sources.holders();
    Ok((sources, holders))
}

/// A funder that is never asked: every scripted deposit is huge.
fn no_topups() -> decdn_client::source::FakeFunder {
    decdn_client::source::FakeFunder::new(
        0,
        decdn_incentive::DepositOutcome::Added(alloy::primitives::U256::ZERO),
    )
}

/// A drive config whose working deposit never gates a scripted fetch.
fn drive_config() -> decdn_client::DriveConfig {
    decdn_client::DriveConfig {
        working_deposit: alloy::primitives::U256::from(u128::MAX),
        seller_reserve: alloy::primitives::U256::ZERO,
        max_settle_waits: 2,
        settle_backoff: std::time::Duration::ZERO,
    }
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
        .drive_config(drive_config())
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

/// A plain entry's line keeps its one rate, taken from what it downloaded.
#[test]
fn entry_done_line_rates_a_plain_entry_on_its_download() {
    let line = entry_done_line(
        [1; 32],
        4 << 20,
        EntryBytes {
            paid: 4 << 20,
            spliced: 0,
            resumed: 0,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(line.ends_with(": 4.00 MiB in 2.0s (2.00 MiB/s)"), "{line}");
}

/// A range-dedup entry's rate counts only its downloaded bytes, and its
/// spliced bytes are reported apart (#2189).
#[test]
fn entry_done_line_splits_a_dedup_entrys_spliced_bytes() {
    let line = entry_done_line(
        [1; 32],
        12 << 20,
        EntryBytes {
            paid: 2 << 20,
            spliced: 10 << 20,
            resumed: 0,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(
            ": 12.00 MiB in 2.0s (2.00 MiB downloaded at 1.00 MiB/s, 10.00 MiB spliced \
             from disk)"
        ),
        "{line}"
    );
}

/// An entry spliced whole from disk downloads nothing and has no rate.
#[test]
fn entry_done_line_renders_a_fully_spliced_entry() {
    let line = entry_done_line(
        [1; 32],
        12 << 20,
        EntryBytes {
            paid: 0,
            spliced: 12 << 20,
            resumed: 0,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(": 12.00 MiB in 2.0s (0 B downloaded at --, 12.00 MiB spliced from disk)"),
        "{line}"
    );
}

/// A resumed entry's rate counts only what this run downloaded, and its
/// resumed and spliced bytes are reported apart (#2236).
#[test]
fn entry_done_line_splits_a_resumed_entrys_bytes() {
    let line = entry_done_line(
        [1; 32],
        12 << 20,
        EntryBytes {
            paid: 2 << 20,
            spliced: 6 << 20,
            resumed: 4 << 20,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(
            ": 12.00 MiB in 2.0s (2.00 MiB downloaded at 1.00 MiB/s, 4.00 MiB resumed, \
             6.00 MiB spliced from disk)"
        ),
        "{line}"
    );

    let line = entry_done_line(
        [1; 32],
        6 << 20,
        EntryBytes {
            paid: 2 << 20,
            spliced: 0,
            resumed: 4 << 20,
        },
        std::time::Duration::from_secs(2),
    );
    assert!(
        line.ends_with(": 6.00 MiB in 2.0s (2.00 MiB downloaded at 1.00 MiB/s, 4.00 MiB resumed)"),
        "{line}"
    );
}

/// An entry that took no measurable time renders the placeholder rate.
#[test]
fn entry_done_line_guards_a_zero_elapsed() {
    let line = entry_done_line(
        [1; 32],
        1 << 20,
        EntryBytes {
            paid: 1 << 20,
            spliced: 0,
            resumed: 0,
        },
        std::time::Duration::ZERO,
    );
    assert!(line.ends_with(": 1.00 MiB in 0.0s (--)"), "{line}");
}
