use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::DepositOutcome;

use super::DownloadTarget;
use super::Downloader;
use crate::driver::DriveConfig;
use crate::source::{BlobSource, FakeFunder, ScriptedSource};
use crate::{
    ClientRangedStore, Cumulative, GaveUp, PoolContext, PoolLedger, ProgressClock, StaticSources,
    StopPolicy, StreamCandidate,
};

/// A buyer context with a huge deposit so funding never gates the fetch,
/// paying `provider` (distinct bytes give the one-lane-per-provider set the
/// scheduler requires).
fn ctx_for(provider: u8) -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        provider: Address::repeat_byte(provider),
        deposit: U256::from(u128::MAX),
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
        capability: None,
    }
}

fn candidate<S>(source: S, ledger: Arc<PoolLedger>, provider: u8) -> StreamCandidate<S> {
    StreamCandidate {
        source,
        ctx: Arc::new(Mutex::new(ctx_for(provider))),
        ledger,
        coverage: None,
        lease: crate::LaneLease::default(),
        widen: None,
    }
}

fn funder() -> FakeFunder {
    FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)))
}

fn drive_config() -> DriveConfig {
    DriveConfig {
        working_deposit: U256::from(u128::MAX),
        seller_reserve: U256::ZERO,
        max_settle_waits: 2,
        settle_backoff: Duration::ZERO,
    }
}

/// A downloader over the static lanes `candidates`.
fn downloader<S: BlobSource>(
    candidates: Vec<StreamCandidate<S>>,
) -> anyhow::Result<Downloader<StaticSources<S>, FakeFunder>> {
    let sources = StaticSources::new(candidates)?;
    let holders = sources.holders();
    let lanes = holders.len();
    Ok(Downloader::new(
        sources,
        holders,
        Arc::default(),
        funder(),
        drive_config(),
        lanes,
    ))
}

/// The lane cap is the caller's, not the holder count: two holders under
/// a cap of one stream through one lane, and the other waits as a
/// reserve.
#[tokio::test]
async fn a_downloader_streams_at_most_max_lanes() -> anyhow::Result<()> {
    let blob = payload(4 * 1024 * 1024);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_b));
    let (root, total) = (src_a.root(), src_a.total_bytes());
    let (probe_a, probe_b) = (src_a.clone(), src_b.clone());
    let sources = StaticSources::new(vec![
        candidate(src_a, ledger_a, 0xA1),
        candidate(src_b, ledger_b, 0xB2),
    ])?;
    let holders = sources.holders();
    let downloader = Downloader::new(
        sources,
        holders,
        Arc::default(),
        funder(),
        drive_config(),
        1,
    );
    let dir = tempfile::tempdir()?;
    downloader
        .fetch_to_dir(&[(root, total)], dir.path(), None)
        .await?;
    anyhow::ensure!(
        (probe_a.delivered_bytes() > 0) != (probe_b.delivered_bytes() > 0),
        "exactly one lane streamed (a={}, b={})",
        probe_a.delivered_bytes(),
        probe_b.delivered_bytes()
    );
    Ok(())
}

/// A deterministic payload spanning several chunk groups and one voucher
/// interval boundary.
fn payload(len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut x: u32 = 0x1234_5678;
    for b in &mut out {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes()[0];
    }
    out
}

/// `fetch_to_dir` writes each blob to `dir/<hex>` and the file is
/// BLAKE3-identical to the source blob (the whole-blob bao root IS its BLAKE3
/// hash, so the promoted file's hash must equal the entry hash).
#[tokio::test]
async fn fetch_to_dir_writes_a_blake3_identical_file() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;
    let paths = downloader
        .fetch_to_dir(&[(root, total)], dir.path(), None)
        .await?;

    anyhow::ensure!(
        paths.len() == 1,
        "one entry → one file, got {}",
        paths.len()
    );
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
    let got = std::fs::read(path)?;
    anyhow::ensure!(
        got == blob,
        "written file must be byte-identical to the blob"
    );
    anyhow::ensure!(
        blake3::hash(&got).as_bytes() == &root,
        "written file must be BLAKE3-identical to the entry hash"
    );
    // The file is named by its content address.
    anyhow::ensure!(
        path.file_name().and_then(|n| n.to_str())
            == Some(blake3::Hash::from_bytes(root).to_hex().as_str()),
        "file is named by its content-address hex"
    );
    Ok(())
}

/// `fetch_to_dir` forwards the loop's per-blob content progress to the
/// caller's callback, and the final report reaches the blob's total — the
/// signal a CLI draws its download bar from.
#[tokio::test]
async fn fetch_to_dir_reports_progress_up_to_the_blobs_total() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;

    let max_pos = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let saw_blob_total = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mp = Arc::clone(&max_pos);
    let sbt = Arc::clone(&saw_blob_total);
    let on_progress = move |pos: u64, tot: u64| {
        mp.fetch_max(pos, std::sync::atomic::Ordering::SeqCst);
        if tot == total {
            sbt.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    };

    downloader
        .fetch_to_dir(&[(root, total)], dir.path(), Some(&on_progress))
        .await?;

    anyhow::ensure!(
        saw_blob_total.load(std::sync::atomic::Ordering::SeqCst),
        "progress must report the blob's content total in the total slot"
    );
    anyhow::ensure!(
        max_pos.load(std::sync::atomic::Ordering::SeqCst) == total,
        "final progress must reach the blob's total content bytes"
    );
    Ok(())
}

/// `fetch_to_paths` writes each blob to its caller-named destination (not a
/// content-hex name), keyed and promoted by that name, and the file is
/// BLAKE3-identical to the source blob (#1848 4c).
#[tokio::test]
async fn fetch_to_paths_writes_each_blob_to_its_named_dest() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("my-model.safetensors");

    let paths = downloader
        .fetch_to_paths(
            &[DownloadTarget {
                hash: root,
                total_bytes: total,
                dest: &dest,
                ranges: None,
            }],
            None,
            None,
        )
        .await?;

    anyhow::ensure!(
        paths == vec![dest.clone()],
        "returns the caller's destination path, not a hex name"
    );
    let got = std::fs::read(&dest)?;
    anyhow::ensure!(got == blob, "written file is byte-identical to the blob");
    anyhow::ensure!(
        blake3::hash(&got).as_bytes() == &root,
        "written file is BLAKE3-identical to the entry hash"
    );
    Ok(())
}

/// A ranged target whose ranges complete the store is promoted: its `dest`
/// exists and is in the result. A ranged target that leaves bytes missing
/// stays a `.partial` and is not in the result.
#[tokio::test]
async fn a_ranged_target_is_promoted_only_when_its_store_completes() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let total = u64::try_from(blob.len())?;
    let dir = tempfile::tempdir()?;

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let part = dir.path().join("part.bin");
    let half = [(0, total / 2)];
    let paths = downloader(vec![candidate(source, ledger, 0xA1)])?
        .fetch_to_paths(
            &[DownloadTarget {
                hash: root,
                total_bytes: total,
                dest: &part,
                ranges: Some(&half),
            }],
            None,
            None,
        )
        .await?;
    anyhow::ensure!(paths.is_empty(), "an incomplete target is not returned");
    anyhow::ensure!(!part.exists(), "an incomplete target is not promoted");

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let whole = dir.path().join("whole.bin");
    let all = [(0, total)];
    let paths = downloader(vec![candidate(source, ledger, 0xA1)])?
        .fetch_to_paths(
            &[DownloadTarget {
                hash: root,
                total_bytes: total,
                dest: &whole,
                ranges: Some(&all),
            }],
            None,
            None,
        )
        .await?;
    anyhow::ensure!(
        paths == vec![whole.clone()],
        "the promoted dest is returned"
    );
    anyhow::ensure!(
        std::fs::read(&whole)? == blob,
        "the promoted file is the blob"
    );
    Ok(())
}

/// `fetch_to_paths` threads a shared `LaneLedgers` registry (a bundle run's
/// pool-wide committed view) into the loop and still fetches byte-identically
/// (#1848 4a).
#[tokio::test]
async fn fetch_to_paths_threads_a_shared_ledger_registry() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("shared-ledger.bin");
    let registry = crate::LaneLedgers::new();

    let paths = downloader
        .fetch_to_paths(
            &[DownloadTarget {
                hash: root,
                total_bytes: total,
                dest: &dest,
                ranges: None,
            }],
            Some(&registry),
            None,
        )
        .await?;

    anyhow::ensure!(paths == vec![dest.clone()]);
    anyhow::ensure!(std::fs::read(&dest)? == blob);
    Ok(())
}

/// Two holders stripe one blob in parallel: the promoted file is
/// BLAKE3-identical and both holders contributed.
#[tokio::test]
async fn two_holders_stripe_a_blake3_identical_file() -> anyhow::Result<()> {
    let blob = payload(4 * 1024 * 1024);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let total = src_a.total_bytes();
    let probe_a = src_a.clone();
    let probe_b = src_b.clone();

    let downloader = downloader(vec![
        candidate(src_a, ledger_a, 0xA1),
        candidate(src_b, ledger_b, 0xB2),
    ])?;
    let dir = tempfile::tempdir()?;
    let paths = downloader
        .fetch_to_dir(&[(root, total)], dir.path(), None)
        .await?;

    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
    anyhow::ensure!(
        std::fs::read(path)? == blob,
        "striped file must be identical"
    );
    anyhow::ensure!(
        probe_a.delivered_bytes() > 0 && probe_b.delivered_bytes() > 0,
        "both holders must have contributed (a={}, b={})",
        probe_a.delivered_bytes(),
        probe_b.delivered_bytes()
    );
    Ok(())
}

/// A pre-seeded `.partial` prefix (a bundle layer's chunk-hint dedup, or a
/// resumed download) is not re-fetched: the source opens only the complement,
/// and the promoted file is still identical.
#[tokio::test]
async fn a_pre_seeded_prefix_is_not_refetched() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = source.total_bytes();
    let probe = source.clone();

    let dir = tempfile::tempdir()?;
    let stem = blake3::Hash::from_bytes(root).to_hex();
    // Seed the first ~half on disk, as a chunk-hint donor or a prior run would.
    let seeded_prefix = total / 2;
    ClientRangedStore::seed_checkpointed_prefix(dir.path(), stem.as_str(), &blob, seeded_prefix)?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let paths = downloader
        .fetch_to_dir(&[(root, total)], dir.path(), None)
        .await?;

    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
    anyhow::ensure!(
        std::fs::read(path)? == blob,
        "resumed file must be identical"
    );
    let opened = probe.opened_ranges();
    anyhow::ensure!(
        opened.iter().all(|&(start, _)| start >= seeded_prefix),
        "the seeded prefix must not be re-opened, opened={opened:?}"
    );
    anyhow::ensure!(
        probe.opened_bytes() < total,
        "a dedup'd download must fetch fewer than the whole blob's bytes ({} of {total})",
        probe.opened_bytes()
    );
    Ok(())
}

/// Fetch `ranges` of `blob` to `dest` under a first claim of `claim`,
/// from one fresh source. Returns the source, so the test can read what it
/// opened, and the promoted paths.
async fn fetch_ranges(
    blob: &[u8],
    dest: &std::path::Path,
    claim: u64,
    ranges: Option<&[(u64, u64)]>,
) -> anyhow::Result<(ScriptedSource, Vec<std::path::PathBuf>)> {
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.to_vec())?.paying(Arc::clone(&ledger));
    let probe = source.clone();
    let paths = downloader(vec![candidate(source, ledger, 0xA1)])?
        .fetch_to_paths(
            &[DownloadTarget {
                hash: probe.root(),
                total_bytes: claim,
                dest,
                ranges,
            }],
            None,
            None,
        )
        .await?;
    Ok((probe, paths))
}

/// A resumed record whose bound is past the first claim finishes: the
/// whole-blob target reaches the record's bound, the file is the blob, and
/// no byte the record holds is opened again.
#[tokio::test]
async fn a_resumed_record_with_a_bound_past_the_claim_finishes() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let held = 1024 * 1024;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    // A first run under the true size lands `[0, held)` and proves nothing.
    let (_, paths) = fetch_ranges(&blob, &dest, total, Some(&[(0, held)])).await?;
    anyhow::ensure!(paths.is_empty(), "the first run leaves a partial");

    // The rerun claims only `held` bytes: the record's bound wins.
    let (probe, paths) = fetch_ranges(&blob, &dest, held, None).await?;
    anyhow::ensure!(paths == vec![dest.clone()], "the rerun promotes");
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    let opened = probe.opened_ranges();
    anyhow::ensure!(
        opened.iter().all(|&(start, _)| start >= held),
        "no held byte is opened again: {opened:?}"
    );
    Ok(())
}

/// A resumed record whose proven size is past the first claim finishes
/// the same way, and opens only the bytes the record lacks.
#[tokio::test]
async fn a_resumed_record_with_a_proven_size_past_the_claim_finishes() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let held = 1024 * 1024;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    // A first run lands the head and the final group, which proves the size.
    let first = [(0, held), (total - group, group)];
    let (_, paths) = fetch_ranges(&blob, &dest, total, Some(&first)).await?;
    anyhow::ensure!(paths.is_empty(), "the first run leaves a partial");

    let (probe, paths) = fetch_ranges(&blob, &dest, held, None).await?;
    anyhow::ensure!(paths == vec![dest.clone()], "the rerun promotes");
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    let opened = probe.opened_ranges();
    anyhow::ensure!(
        opened
            .iter()
            .all(|&(start, len)| start >= held && start + len <= total - group),
        "only the missing middle is opened: {opened:?}"
    );
    Ok(())
}

/// Bytes that drift on disk after they verified fail the finalize hash.
/// The same fetch runs once more over the blob and completes.
#[tokio::test]
async fn a_finalize_hash_mismatch_fetches_again_and_completes() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    // A record that claims and proves every byte over a data file with one
    // flipped byte: the drift the finalize hash exists to catch.
    let mut drifted = blob.clone();
    if let Some(b) = drifted.get_mut(1000) {
        *b ^= 0xFF;
    }
    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "model.bin", &drifted, total)?;

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let probe = source.clone();
    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let target = DownloadTarget {
        hash: probe.root(),
        total_bytes: total,
        dest: &dest,
        ranges: None,
    };
    let paths = downloader.fetch_to_paths(&[target], None, None).await?;
    anyhow::ensure!(paths == vec![dest.clone()], "the fetch promotes");
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    anyhow::ensure!(
        probe.opened_bytes() >= total,
        "the second pass fetched the blob again: {}",
        probe.opened_bytes()
    );
    anyhow::ensure!(
        downloader.refetched_targets() == 1,
        "the downloader reports the target it fetched again"
    );
    Ok(())
}

/// A lone source that stalls mid-blob trips the lane watchdog, cools, and
/// comes back to finish the blob from the gap: one holder recovers.
#[tokio::test(start_paused = true)]
async fn a_lone_stalled_source_recovers_after_it_cools() -> anyhow::Result<()> {
    let blob = payload(12 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?
        .stall_after(6 * 1024 * 1024, Duration::from_hours(1))
        .paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    let stop = StopPolicy::new(false, None, Arc::new(ProgressClock::new()));
    downloader
        .fetch_to_paths_until(
            &[DownloadTarget {
                hash: root,
                total_bytes: total,
                dest: &dest,
                ranges: None,
            }],
            None,
            None,
            &stop,
        )
        .await?;
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file must be identical");
    Ok(())
}

/// A download whose lone source never delivers gives up once the stop
/// policy's limit passes without a verified byte, and the target is not
/// promoted.
#[tokio::test(start_paused = true)]
async fn a_download_with_no_progress_gives_up() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?
        .paying(Arc::clone(&ledger))
        .with_fault_after(0, || anyhow::anyhow!("connection reset"));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    let limit = Duration::from_secs(30);
    let stop = StopPolicy::new(false, Some(limit), Arc::new(ProgressClock::new()));
    let fetched = downloader
        .fetch_to_paths_until(
            &[DownloadTarget {
                hash: root,
                total_bytes: total,
                dest: &dest,
                ranges: None,
            }],
            None,
            None,
            &stop,
        )
        .await;
    let Err(err) = fetched else {
        anyhow::bail!("a download with no progress must give up");
    };
    anyhow::ensure!(
        err.downcast_ref::<GaveUp>() == Some(&GaveUp { idle: limit }),
        "{err:#}"
    );
    anyhow::ensure!(!dest.exists(), "a stopped target is not promoted");
    Ok(())
}

/// A download dropped mid-fetch (a caller's Ctrl-C) before any periodic
/// flush still records the bytes that landed, so a resume does not fetch
/// and pay for them again.
#[tokio::test(start_paused = true)]
async fn a_dropped_download_records_the_landed_prefix() -> anyhow::Result<()> {
    const STALL: u64 = 6 * 1024 * 1024;
    let blob = payload(12 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?
        .stall_after(STALL, Duration::from_hours(1))
        .paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    let targets = [DownloadTarget {
        hash: root,
        total_bytes: total,
        dest: &dest,
        ranges: None,
    }];
    // Well inside the first periodic flush, so only the drop can record.
    let dropped = tokio::time::timeout(
        crate::driver::PRESENT_RECORD_FLUSH_INTERVAL / 5,
        downloader.fetch_to_paths(&targets, None, None),
    )
    .await;
    anyhow::ensure!(
        dropped.is_err(),
        "the stalled download must still be running"
    );

    let partial = ClientRangedStore::open(dir.path(), "model.bin", root)?;
    let recorded = crate::driver::ranges_content_len(
        &decdn_bao_range::RangedStore::present_ranges(&partial).await?,
        total,
    );
    let landed = ClientRangedStore::checkpointed_len(STALL);
    anyhow::ensure!(
        recorded >= landed && recorded < total,
        "the landed prefix is recorded for a resume: {recorded}, want at least {landed}"
    );
    Ok(())
}

/// Static sources with no candidate fail early with a clear message: their
/// discovery could never find a holder.
#[tokio::test]
async fn empty_static_sources_are_a_clear_error() -> anyhow::Result<()> {
    let Err(err) = downloader::<ScriptedSource>(Vec::new()) else {
        anyhow::bail!("empty static sources must be rejected");
    };
    anyhow::ensure!(
        err.to_string().contains("at least one candidate"),
        "the error must name the empty candidate set, got: {err}"
    );
    Ok(())
}

/// A downloader that starts with no holder discovers them and fetches.
#[tokio::test(start_paused = true)]
async fn a_downloader_with_no_holder_discovers_them() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;
    let sources = StaticSources::new(vec![candidate(source, ledger, 0xA1)])?;
    let downloader = Downloader::new(
        sources,
        Vec::new(),
        Arc::default(),
        funder(),
        drive_config(),
        1,
    );
    let dir = tempfile::tempdir()?;
    let paths = downloader
        .fetch_to_dir(&[(root, total)], dir.path(), None)
        .await?;
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
    anyhow::ensure!(std::fs::read(path)? == blob);
    Ok(())
}
