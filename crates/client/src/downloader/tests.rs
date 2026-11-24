use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::ranged_store::{BlobDir, StaleStoreEntry};

use super::DownloadTarget;
use super::Downloader;
use crate::source::{BlobSource, FakeFunder, PrimedSource, Recovery, ScriptedSource};
use crate::{
    ClientRangedStore, Cumulative, GaveUp, PoolContext, PoolLedger, ProgressClock, ProvenSize,
    StaticSources, StopPolicy, StreamCandidate,
};
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;

/// A buyer context with a huge deposit so funding never gates the fetch,
/// paying `provider` (distinct bytes give the one-lane-per-provider set the
/// scheduler requires).
fn ctx_for(provider: u8) -> PoolContext {
    PoolContext {
        pool_id: B256::ZERO,
        provider: Address::repeat_byte(provider),
        deposit: U256::from(u128::MAX),
        client_signer: Arc::new(PrivateKeySigner::random()),
        voucher_domain: decdn_incentive::capacity_bond_domain(1, Address::ZERO),
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
    FakeFunder::new(Recovery::ToppedUp(U256::from(u128::MAX)))
}

/// A downloader over the static lanes `candidates`.
fn downloader<S: BlobSource>(
    candidates: Vec<StreamCandidate<S>>,
) -> anyhow::Result<Downloader<StaticSources<S>, FakeFunder>> {
    let sources = StaticSources::new(candidates)?;
    let holders = sources.holders();
    Ok(Downloader::new(sources, funder()).holders(holders))
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
    let downloader = Downloader::new(sources, funder())
        .holders(holders)
        .max_lanes(1);
    let dir = tempfile::tempdir()?;
    downloader
        .fetch_to_dir(
            &[(root, ProvenSize::assume_for_tests(total))],
            dir.path(),
            None,
        )
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
        .fetch_to_dir(
            &[(root, ProvenSize::assume_for_tests(total))],
            dir.path(),
            None,
        )
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

/// A lane's whole-blob pull opened for the first size claim and primed on
/// it (`LaneSources::first_claim`) is the download's first leg: the drive
/// adopts it, so the source sees no open but the priming one (#2633).
#[tokio::test]
async fn a_primed_whole_blob_pull_is_the_downloads_first_leg() -> anyhow::Result<()> {
    for len in [1234, 1_500_000] {
        let blob = payload(len);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
        let (root, total) = (source.root(), source.total_bytes());
        let probe = source.clone();
        let primed = PrimedSource::new(source);
        let range = decdn_bao_range::align_range(0, 0, total)?;
        let (header, reader) = primed.inner().open(root, range.clone()).await?;
        primed.prime_next(root, range, header, reader, tokio::time::Instant::now());

        let downloader = downloader(vec![candidate(primed, ledger, 0xA1)])?;
        let dir = tempfile::tempdir()?;
        let paths = downloader
            .fetch_to_dir(
                &[(root, ProvenSize::assume_for_tests(total))],
                dir.path(),
                None,
            )
            .await?;
        let path = paths
            .first()
            .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
        anyhow::ensure!(std::fs::read(path)? == blob, "{len}: the file is the blob");
        anyhow::ensure!(
            probe.opened_ranges() == vec![(0, total)],
            "{len}: only the priming open reached the source, got {:?}",
            probe.opened_ranges()
        );
    }
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
        .fetch_to_dir(
            &[(root, ProvenSize::assume_for_tests(total))],
            dir.path(),
            Some(&on_progress),
        )
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
                total_bytes: ProvenSize::assume_for_tests(total),
                dest: &dest,
            }],
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

/// `fetch_to_paths_shared` threads a shared `LaneLedgers` registry (a
/// bundle run's pool-wide committed view) into the loop and still fetches
/// byte-identically (#1848 4a).
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
    let stop = StopPolicy::new(true, None, Arc::new(ProgressClock::new()));

    let paths = downloader
        .fetch_to_paths_shared(
            &[DownloadTarget {
                hash: root,
                total_bytes: ProvenSize::assume_for_tests(total),
                dest: &dest,
            }],
            &registry,
            None,
            &stop,
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
        .fetch_to_dir(
            &[(root, ProvenSize::assume_for_tests(total))],
            dir.path(),
            None,
        )
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

/// A prefix an earlier run left in the store is not re-fetched: the source
/// opens only the complement, and the exported file is still identical.
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
    // Seed the first ~half into the store, as a prior run would.
    let seeded_prefix = total / 2;
    ClientRangedStore::seed_download_prefix(&dir.path().join(stem.as_str()), &blob, seeded_prefix)
        .await?;

    let downloader = downloader(vec![candidate(source, ledger, 0xA1)])?;
    let paths = downloader
        .fetch_to_dir(
            &[(root, ProvenSize::assume_for_tests(total))],
            dir.path(),
            None,
        )
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

/// Fetch `blob` to `dest` under a first claim of `claim`, from one fresh
/// source. Returns the source, so the test can read what it opened, and the
/// exported paths.
async fn fetch_whole(
    blob: &[u8],
    dest: &std::path::Path,
    claim: u64,
) -> anyhow::Result<(ScriptedSource, Vec<std::path::PathBuf>)> {
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.to_vec())?.paying(Arc::clone(&ledger));
    let probe = source.clone();
    let paths = downloader(vec![candidate(source, ledger, 0xA1)])?
        .fetch_to_paths(
            &[DownloadTarget {
                hash: probe.root(),
                total_bytes: ProvenSize::assume_for_tests(claim),
                dest,
            }],
            None,
        )
        .await?;
    Ok((probe, paths))
}

/// A resumed store that holds its head and its final group opens only the
/// missing middle.
#[tokio::test]
async fn a_resumed_record_with_its_tail_opens_only_the_middle() -> anyhow::Result<()> {
    let blob = payload(3 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let held = 1024 * 1024;
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    // An earlier, interrupted run landed the head and the final group.
    let store_dir = BlobDir::open(&crate::ranged_store::store_dir_for(&dest)?).await?;
    let root = *blake3::hash(&blob).as_bytes();
    let store = store_dir.blob(root, total, None).await?;
    store.admit_range(&blob, 0, held).await?;
    store.admit_range(&blob, total - group, group).await?;
    store_dir.shutdown().await?;

    let (probe, paths) = fetch_whole(&blob, &dest, total).await?;
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

/// A destination that already holds the blob (an earlier run exported it and
/// stopped before it removed its store) is not fetched again, and the
/// leftover store goes.
#[tokio::test]
async fn an_exported_dest_is_not_fetched_again() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    std::fs::write(&dest, &blob)?;
    ClientRangedStore::seed_download_prefix(&dest, &blob, total).await?;
    let store_dir = crate::ranged_store::store_dir_for(&dest)?;
    anyhow::ensure!(store_dir.exists(), "the seeded store is on disk");

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let probe = source.clone();
    let target = DownloadTarget {
        hash: probe.root(),
        total_bytes: ProvenSize::assume_for_tests(total),
        dest: &dest,
    };
    let paths = downloader(vec![candidate(source, ledger, 0xA1)])?
        .fetch_to_paths(&[target], None)
        .await?;
    anyhow::ensure!(
        paths == vec![dest.clone()],
        "the target is reported written"
    );
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    anyhow::ensure!(probe.opened_bytes() == 0, "nothing is fetched");
    anyhow::ensure!(!store_dir.exists(), "the leftover store is removed");
    Ok(())
}

/// Bytes that change in the store's data file after they verified fail the
/// exported file's hash. The file is removed, and the same fetch runs once
/// more from an empty store and completes.
#[tokio::test]
async fn an_export_that_fails_its_hash_fetches_again() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    ClientRangedStore::seed_download_prefix(&dest, &blob, total).await?;
    let data_dir = crate::ranged_store::store_dir_for(&dest)?.join("data");
    let data_file = std::fs::read_dir(&data_dir)?
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "data"))
        .ok_or_else(|| anyhow::anyhow!("no data file in {}", data_dir.display()))?;
    let mut drifted = std::fs::read(&data_file)?;
    if let Some(b) = drifted.get_mut(1000) {
        *b ^= 0xFF;
    }
    std::fs::write(&data_file, &drifted)?;

    let (probe, paths) = fetch_whole(&blob, &dest, total).await?;
    anyhow::ensure!(paths == vec![dest.clone()], "the fetch exports");
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    anyhow::ensure!(
        probe.opened_bytes() >= total,
        "the second pass fetched the blob again: {}",
        probe.opened_bytes()
    );
    Ok(())
}

/// The whole-file hash spans every read: a file several reads long that
/// holds the blob matches, one with a byte changed in its last read does not,
/// and a missing file holds nothing.
#[tokio::test]
async fn dest_holds_hashes_every_read_of_the_file() -> anyhow::Result<()> {
    let len = 2 * super::HASH_READ_BYTES + 12_345;
    let blob = payload(len);
    let hash = *blake3::hash(&blob).as_bytes();
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("big.bin");
    anyhow::ensure!(!super::dest_holds(&dest, hash).await, "no file");
    std::fs::write(&dest, &blob)?;
    anyhow::ensure!(super::dest_holds(&dest, hash).await, "the blob matches");
    let mut changed = blob;
    if let Some(b) = changed.last_mut() {
        *b ^= 0xFF;
    }
    std::fs::write(&dest, &changed)?;
    anyhow::ensure!(
        !super::dest_holds(&dest, hash).await,
        "a changed last read does not match"
    );
    Ok(())
}

/// A finished download exports its blob and removes its store.
#[tokio::test]
async fn a_finished_download_removes_its_store() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    let (_probe, paths) = fetch_whole(&blob, &dest, total).await?;
    anyhow::ensure!(paths == vec![dest.clone()], "the fetch exports");
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    anyhow::ensure!(
        !crate::ranged_store::store_dir_for(&dest)?.exists(),
        "the store is removed"
    );
    Ok(())
}

/// Fetch the whole `blob` to `dest` through the caller's `store`.
async fn fetch_into(store: &BlobDir, blob: &[u8], dest: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let total = u64::try_from(blob.len())?;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.to_vec())?.paying(Arc::clone(&ledger));
    let target = DownloadTarget {
        hash: source.root(),
        total_bytes: ProvenSize::assume_for_tests(total),
        dest,
    };
    downloader(vec![candidate(source, ledger, 0xA1)])?
        .store(store.clone())
        .fetch_to_paths(&[target], None)
        .await
}

/// A download through a caller's store exports its blob and leaves the store
/// in place. Once the exported file is gone, the store holds a lost export,
/// and a fetch of that blob through it ends with `StaleStoreEntry`.
#[tokio::test]
async fn a_callers_store_stays_and_a_lost_export_is_stale() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    let store = BlobDir::open(&dir.path().join("store")).await?;

    let paths = fetch_into(&store, &blob, &dest).await?;
    anyhow::ensure!(paths == vec![dest.clone()], "the fetch exports");
    anyhow::ensure!(std::fs::read(&dest)? == blob, "the file is the blob");
    anyhow::ensure!(
        dir.path().join("store").exists(),
        "the caller's store stays"
    );
    anyhow::ensure!(
        !crate::ranged_store::store_dir_for(&dest)?.exists(),
        "no store beside dest"
    );

    std::fs::remove_file(&dest)?;
    let err = fetch_into(&store, &blob, &dest)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a lost export must not fetch"))?;
    anyhow::ensure!(
        err.downcast_ref::<StaleStoreEntry>().is_some(),
        "a lost export is stale: {err:#}"
    );
    store.shutdown().await
}

/// An export through a caller's store that fails its hash cannot fetch
/// again into that store: the target ends with `StaleStoreEntry`.
#[tokio::test]
async fn a_drifted_export_in_a_callers_store_is_stale() -> anyhow::Result<()> {
    let blob = payload(2 * 1024 * 1024);
    let total = u64::try_from(blob.len())?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("model.bin");
    let store = BlobDir::open(&dir.path().join("store")).await?;
    let root = *blake3::hash(&blob).as_bytes();
    store
        .blob(root, total, None)
        .await?
        .admit_prefix(&blob, total)
        .await?;
    let data_dir = dir.path().join("store").join("data");
    let data_file = std::fs::read_dir(&data_dir)?
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "data"))
        .ok_or_else(|| anyhow::anyhow!("no data file in {}", data_dir.display()))?;
    let mut drifted = std::fs::read(&data_file)?;
    if let Some(b) = drifted.get_mut(1000) {
        *b ^= 0xFF;
    }
    std::fs::write(&data_file, &drifted)?;

    let err = fetch_into(&store, &blob, &dest)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a drifted export must not land"))?;
    anyhow::ensure!(
        err.downcast_ref::<StaleStoreEntry>().is_some(),
        "a drifted export is stale: {err:#}"
    );
    anyhow::ensure!(!dest.exists(), "the drifted file is removed");
    store.shutdown().await
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
                total_bytes: ProvenSize::assume_for_tests(total),
                dest: &dest,
            }],
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
                total_bytes: ProvenSize::assume_for_tests(total),
                dest: &dest,
            }],
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
/// flush still keeps the bytes that landed in its store, so a resume does
/// not fetch and pay for them again. On the real clock: the file store runs
/// on its own runtime, which a paused clock does not wait for.
#[tokio::test]
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
        total_bytes: ProvenSize::assume_for_tests(total),
        dest: &dest,
    }];
    // Well inside the first periodic flush, so only the drop can record.
    let dropped = tokio::time::timeout(
        crate::driver::PRESENT_RECORD_FLUSH_INTERVAL / 5,
        downloader.fetch_to_paths(&targets, None),
    )
    .await;
    anyhow::ensure!(
        dropped.is_err(),
        "the stalled download must still be running"
    );

    let store_dir = reopen(&crate::ranged_store::store_dir_for(&dest)?).await?;
    let partial = store_dir.blob(root, total, None).await?;
    let recorded = crate::driver::ranges_content_len(
        &decdn_bao_range::RangedStore::present_ranges(&partial).await?,
        total,
    );
    store_dir.shutdown().await?;
    let landed = ClientRangedStore::checkpointed_len(STALL);
    anyhow::ensure!(
        recorded >= landed && recorded < total,
        "the landed prefix is recorded for a resume: {recorded}, want at least {landed}"
    );
    Ok(())
}

/// Open the store in `dir` that a dropped download left. A dropped store
/// releases its database in the background, so the open retries for a short
/// while on the real clock.
async fn reopen(dir: &std::path::Path) -> anyhow::Result<crate::ranged_store::BlobDir> {
    let mut last = None;
    for _ in 0..100 {
        match crate::ranged_store::BlobDir::open(dir).await {
            Ok(store) => return Ok(store),
            Err(e) => last = Some(e),
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no open attempted")))
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

/// A downloader built with no setting at all discovers its holders and
/// fetches.
#[tokio::test(start_paused = true)]
async fn a_downloader_with_no_holder_discovers_them() -> anyhow::Result<()> {
    let blob = payload(1_500_000);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
    let root = source.root();
    let total = u64::try_from(blob.len())?;
    let sources = StaticSources::new(vec![candidate(source, ledger, 0xA1)])?;
    let downloader = Downloader::new(sources, funder());
    let dir = tempfile::tempdir()?;
    let paths = downloader
        .fetch_to_dir(
            &[(root, ProvenSize::assume_for_tests(total))],
            dir.path(),
            None,
        )
        .await?;
    let path = paths
        .first()
        .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
    anyhow::ensure!(std::fs::read(path)? == blob);
    Ok(())
}
