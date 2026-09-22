//! The `Downloader` consumption face (#1848 T4): fetch a set of content-addressed
//! blobs — a bundle, or a single blob — to files in a directory.
//!
//! A `Downloader` is an OUTPUT + SCHEDULING adapter over the ONE shared
//! multi-source core, not a second engine: each entry opens a
//! [`ClientRangedStore`] beside its file and fetches its missing ranges across
//! the candidate set through `multi_source_fetch` — bao-verifying every byte,
//! striping across every holder at full throughput, and failing over between
//! them — then finalizes, promoting the `.partial` to the final file. So a
//! resumed download re-pulls only what it lacks, and a bundle layer above can
//! pre-seed held ranges (its chunk-hint dedup) into the `.partial` and the same
//! fetch fills only the complement.
//!
//! It is the download half of the same core the `Streamer` (the consumption-paced
//! single-blob face) uses — the Downloader just uncaps the fan-out and drops the
//! consumer pacing. Bundles are the main scenario: a bundle is a set of blobs, so
//! both entry points take a slice and write one file per blob. `fetch_to_paths`
//! writes each blob to a caller-chosen destination (a bundle's manifest paths, or
//! a single `decdn fetch -o` target); `fetch_to_dir` is the convenience over it
//! that names each file by its content-address hex under a directory. A single
//! blob is a bundle of one.

use std::path::{Path, PathBuf};

use crate::driver::DriveConfig;
use crate::pacer::BudgetPacer;
use crate::scheduler::{MultiSourceConfig, multi_source_fetch};
use crate::source::{BlobSource, Funder};
use crate::streamer::{StreamCandidate, source_lanes};
use crate::{ClientRangedStore, ProgressCallback, PullConfig, RangedStore};

/// One blob to fetch and where to write it: the content `hash` (the bao root),
/// its `total_bytes` (authoritative for keying the store and sizing the fetch),
/// and the caller-chosen `dest` path the `.partial` and final file are keyed by.
#[derive(Debug, Clone, Copy)]
pub struct DownloadTarget<'a> {
    /// The blob's BLAKE3 content address (its bao root); every ingested byte is
    /// verified against it.
    pub hash: [u8; 32],
    /// The blob's content length in bytes.
    pub total_bytes: u64,
    /// Where to write the finished blob. The `.partial` store and the promoted
    /// final file are keyed by this path, so the finished file IS `dest`.
    pub dest: &'a Path,
}

/// Fetch content-addressed blobs — a bundle, or a single blob — to files in a
/// directory, reusing the shared multi-source core (`multi_source_fetch` +
/// [`ClientRangedStore`]) at full throughput.
///
/// A `Downloader` is the download face over the same engine the [`crate::Streamer`]
/// uses: it fetches across the injected [`StreamCandidate`] set with the fan-out
/// UNCAPPED (every candidate) and no consumption pacing, then promotes each
/// finished blob to its file. One candidate set — one dial and one payment
/// channel per provider — serves a whole bundle (each [`BlobSource::open`] takes
/// the hash). A candidate that faults is failed over to another (shared-pool
/// failover, #1174); a resumed download (an existing `.partial`, e.g. a bundle
/// layer's chunk-hint dedup) re-pulls only its missing ranges.
pub struct Downloader<S, F> {
    /// The provider candidates every entry is fetched across.
    candidates: Vec<StreamCandidate<S>>,
    /// The top-up seam a mid-fetch cap exhaustion funds through.
    funder: F,
    /// The driver's funding/settle policy.
    drive_config: DriveConfig,
}

impl<S, F> std::fmt::Debug for Downloader<S, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Downloader")
            .field("candidates", &self.candidates.len())
            .field("drive_config", &self.drive_config)
            .finish_non_exhaustive()
    }
}

impl<S, F> Downloader<S, F> {
    /// Build a downloader over the injected provider candidates. The same set
    /// serves every entry a later `fetch_to_dir` fetches; each candidate must name
    /// a distinct on-chain provider.
    #[must_use]
    pub const fn new(
        candidates: Vec<StreamCandidate<S>>,
        funder: F,
        drive_config: DriveConfig,
    ) -> Self {
        Self {
            candidates,
            funder,
            drive_config,
        }
    }
}

impl<S, F> Downloader<S, F>
where
    S: BlobSource,
    F: Funder,
{
    /// Fetch every `(hash, total_bytes)` entry to a file named by its
    /// content-address hex inside `dir`, returning the written paths in entry
    /// order.
    ///
    /// Each entry opens a [`ClientRangedStore`] beside its file and drives only
    /// its missing ranges across the candidate set through `multi_source_fetch`
    /// (bao-verifying every byte, failing over between candidates), then finalizes
    /// — promoting `.partial` to the final file. Writes land at their absolute
    /// offsets, so out-of-order and multi-source fills assemble correctly; a
    /// resumed download (or a bundle layer's chunk-hint dedup, pre-seeded into the
    /// `.partial`) re-pulls only what it lacks. The whole blob is fetched at full
    /// throughput — `config`'s read-ahead bound is a `Streamer` tunable and does
    /// not apply here.
    ///
    /// `total_bytes` is authoritative for keying the store, and the entry `hash`
    /// is the bao root every ingested byte is verified against: a wrong size or
    /// hash surfaces as a verification failure, never as silent corruption.
    ///
    /// `on_progress`, when set, is called with the core's verified CONTENT
    /// progress for the entry CURRENTLY fetching — `(position, total_bytes)`, both
    /// in content bytes, resetting to that entry's own total at each new entry. A
    /// multi-entry caller drawing one bar accumulates the completed entries' totals
    /// itself; a single-entry download can use it directly.
    ///
    /// # Errors
    ///
    /// A store open/create/finalize I/O error, or any fault `multi_source_fetch`
    /// raises for an entry once every candidate is exhausted (a refused or
    /// underfunded pull, a stalled peer, a verification failure). The first
    /// failing entry aborts the batch; entries already written stay on disk (and
    /// any `.partial` a failed entry left is the resume prefix a retry inherits).
    pub async fn fetch_to_dir(
        &self,
        entries: &[([u8; 32], u64)],
        dir: &Path,
        config: &PullConfig,
        on_progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<Vec<PathBuf>> {
        // Name each blob's destination by its content-address hex under `dir`,
        // then fetch to those explicit paths. A caller that wants its own names
        // (a bundle's manifest paths) uses [`Self::fetch_to_paths`] directly.
        let dests: Vec<PathBuf> = entries
            .iter()
            .map(|&(hash, _)| dir.join(blake3::Hash::from_bytes(hash).to_hex().as_str()))
            .collect();
        let targets: Vec<DownloadTarget<'_>> = entries
            .iter()
            .zip(dests.iter())
            .map(|(&(hash, total_bytes), dest)| DownloadTarget {
                hash,
                total_bytes,
                dest,
            })
            .collect();
        self.fetch_to_paths(&targets, config, on_progress).await
    }

    /// Fetch every [`DownloadTarget`] to its own `dest` path, returning the
    /// written paths in target order. Each blob's `.partial` and final file are
    /// keyed by `dest` (its parent directory and file name), so a resumed
    /// download re-pulls only what it lacks and the promoted file IS `dest` — no
    /// post-finalize rename. The parent directory of each `dest` is created if
    /// missing.
    ///
    /// Each target opens a [`ClientRangedStore`] beside its file and drives only
    /// its missing ranges across the candidate set through `multi_source_fetch`
    /// (bao-verifying every byte, failing over between candidates), then finalizes
    /// — promoting `.partial` to `dest`. Writes land at their absolute offsets, so
    /// out-of-order and multi-source fills assemble correctly; a resumed download
    /// (or a bundle layer's chunk-hint dedup, pre-seeded into the `.partial`)
    /// re-pulls only what it lacks. The whole blob is fetched at full throughput,
    /// with `config.download_unit_deadline` the per-lane stall watchdog.
    ///
    /// `total_bytes` is authoritative for keying the store, and the target `hash`
    /// is the bao root every ingested byte is verified against: a wrong size or
    /// hash surfaces as a verification failure, never as silent corruption.
    ///
    /// `on_progress`, when set, is called with the core's verified CONTENT
    /// progress for the target CURRENTLY fetching — `(position, total_bytes)`,
    /// both content bytes, resetting to that target's own total at each new one.
    ///
    /// # Errors
    ///
    /// A `dest` with no file name, a store open/create/finalize I/O error, or any
    /// fault `multi_source_fetch` raises for a target once every candidate is
    /// exhausted. The first failing target aborts the batch; targets already
    /// written stay on disk (and any `.partial` a failed target left is the resume
    /// prefix a retry inherits).
    pub async fn fetch_to_paths(
        &self,
        targets: &[DownloadTarget<'_>],
        config: &PullConfig,
        on_progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<Vec<PathBuf>> {
        // Fail early and clearly on an empty candidate set, rather than deep inside
        // `multi_source_fetch` on the first target with a less obvious message.
        anyhow::ensure!(
            !self.candidates.is_empty(),
            "a Downloader needs at least one provider candidate to fetch from"
        );
        let pacer = BudgetPacer::new();
        let mut written = Vec::with_capacity(targets.len());
        for target in targets {
            let (dir, stem) = dest_store_location(target.dest)?;
            std::fs::create_dir_all(&dir)
                .map_err(|e| anyhow::anyhow!("create download dir {}: {e}", dir.display()))?;
            let store =
                ClientRangedStore::open_or_create(&dir, &stem, target.hash, target.total_bytes)
                    .map_err(|e| {
                        anyhow::anyhow!("open ranged store for {}: {e}", target.dest.display())
                    })?;
            // One lane per provider, each carrying its candidate's measured
            // coverage (a `None` candidate is a full holder), so a partial holder
            // (#1506) is never assigned a range it does not hold. For a multi-blob
            // download, a candidate that does not fully cover every target must be
            // fetched per blob with per-blob coverage.
            let lanes = source_lanes(&self.candidates, target.total_bytes);
            let ms = MultiSourceConfig {
                // Uncapped fan-out — a download wants every holder striping in
                // parallel, unlike the Streamer's small bounded front.
                max_sources: self.candidates.len().max(1),
                unit_deadline: config.download_unit_deadline,
            };
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &self.funder,
                target.hash,
                0,
                target.total_bytes,
                &self.drive_config,
                &ms,
                on_progress,
                None,
                // No consumption pacing: a download runs at full throughput.
                None,
            )
            .await?;
            // `multi_source_fetch` flushes the present record but does not promote;
            // a download keeps the file, so finalize (verify + promote `.partial`).
            store.finalize().await?;
            written.push(target.dest.to_path_buf());
        }
        Ok(written)
    }
}

/// The directory and stem a [`DownloadTarget`]'s `.partial` store lives under, so
/// its promoted final path IS `dest` (no post-finalize rename): `dest`'s parent
/// (or the current directory for a bare file name) and `dest`'s own file name.
fn dest_store_location(dest: &Path) -> anyhow::Result<(PathBuf, String)> {
    let dir = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let stem = dest
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("download dest {} has no usable file name", dest.display()))?
        .to_string();
    Ok((dir, stem))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::DepositOutcome;

    use super::DownloadTarget;
    use super::Downloader;
    use crate::driver::DriveConfig;
    use crate::source::{FakeFunder, ScriptedSource};
    use crate::{
        ClientRangedStore, Cumulative, PoolContext, PoolLedger, PullConfig, StreamCandidate,
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

        let downloader = Downloader::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
        );
        let dir = tempfile::tempdir()?;
        let paths = downloader
            .fetch_to_dir(&[(root, total)], dir.path(), &PullConfig::default(), None)
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

    /// `fetch_to_dir` forwards the core's per-blob content progress to the
    /// caller's callback, and the final report reaches the blob's total — the
    /// signal a CLI draws its download bar from.
    #[tokio::test]
    async fn fetch_to_dir_reports_progress_up_to_the_blobs_total() -> anyhow::Result<()> {
        let blob = payload(1_500_000);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger));
        let root = source.root();
        let total = u64::try_from(blob.len())?;

        let downloader = Downloader::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
        );
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
                &[(root, total)],
                dir.path(),
                &PullConfig::default(),
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

        let downloader = Downloader::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
        );
        let dir = tempfile::tempdir()?;
        let dest = dir.path().join("my-model.safetensors");

        let paths = downloader
            .fetch_to_paths(
                &[DownloadTarget {
                    hash: root,
                    total_bytes: total,
                    dest: &dest,
                }],
                &PullConfig::default(),
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

    /// Two candidates stripe one blob in parallel: the promoted file is
    /// BLAKE3-identical and both holders contributed (the multi-source fan-out).
    #[tokio::test]
    async fn two_candidates_stripe_a_blake3_identical_file() -> anyhow::Result<()> {
        let blob = payload(4 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(blob.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let probe_a = src_a.clone();
        let probe_b = src_b.clone();

        let downloader = Downloader::new(
            vec![
                candidate(src_a, ledger_a, 0xA1),
                candidate(src_b, ledger_b, 0xB2),
            ],
            funder(),
            drive_config(),
        );
        let dir = tempfile::tempdir()?;
        let paths = downloader
            .fetch_to_dir(&[(root, total)], dir.path(), &PullConfig::default(), None)
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
            "both candidates must have contributed (a={}, b={})",
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
        ClientRangedStore::seed_checkpointed_prefix(
            dir.path(),
            stem.as_str(),
            &blob,
            seeded_prefix,
        )?;

        let downloader = Downloader::new(
            vec![candidate(source, ledger, 0xA1)],
            funder(),
            drive_config(),
        );
        let paths = downloader
            .fetch_to_dir(&[(root, total)], dir.path(), &PullConfig::default(), None)
            .await?;

        let path = paths
            .first()
            .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
        anyhow::ensure!(
            std::fs::read(path)? == blob,
            "resumed file must be identical"
        );
        // Only the complement was fetched — never the seeded prefix. Every opened
        // range starts at or after the seeded frontier, and the total opened is
        // strictly less than the whole blob.
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

    /// An empty candidate set fails early with a clear message, not deep inside the
    /// scheduler.
    #[tokio::test]
    async fn an_empty_candidate_set_is_a_clear_error() -> anyhow::Result<()> {
        let downloader =
            Downloader::<ScriptedSource, FakeFunder>::new(Vec::new(), funder(), drive_config());
        let dir = tempfile::tempdir()?;
        let Err(err) = downloader
            .fetch_to_dir(&[([0u8; 32], 1)], dir.path(), &PullConfig::default(), None)
            .await
        else {
            anyhow::bail!("an empty candidate set must be rejected");
        };
        anyhow::ensure!(
            err.to_string().contains("at least one provider candidate"),
            "the error must name the empty candidate set, got: {err}"
        );
        Ok(())
    }
}
