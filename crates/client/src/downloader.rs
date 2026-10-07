//! The `Downloader` consumption face (#1848): fetch a set of content-addressed
//! blobs — a bundle, or a single blob — to files in a directory.
//!
//! A `Downloader` is an OUTPUT + SCHEDULING adapter over the ONE shared
//! acquire loop, not a second engine: each entry opens a [`ClientRangedStore`]
//! beside its file and fills its missing ranges through [`crate::acquire`]
//! across a [`SourceSet`] of the blob's holders — bao-verifying every byte and
//! striping across every holder at full throughput. A source that faults cools
//! and returns; the fetch ends on done, a fatal fault, or the stop policy. It
//! then finalizes, promoting the `.partial` to the final file. So a resumed
//! download re-pulls only what it lacks, and a bundle layer above can pre-seed
//! held ranges (its chunk-hint dedup) into the `.partial` and the same fetch
//! fills only the complement.
//!
//! It is the download half of the same loop the `Streamer` (the
//! consumption-paced single-blob face) uses — the Downloader just uncaps the
//! lanes and drops the consumer pacing. Bundles are the main scenario: a bundle
//! is a set of blobs, so both entry points take a slice and write one file per
//! blob. `fetch_to_paths` writes each blob to a caller-chosen destination (a
//! bundle's manifest paths, or a single `decdn fetch -o` target); `fetch_to_dir`
//! is the convenience over it that names each file by its content-address hex
//! under a directory. A single blob is a bundle of one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::U256;

use crate::driver::DriveConfig;
use crate::health::PeerHealth;
use crate::ledgers::LaneLedgers;
use crate::pacer::BudgetPacer;
use crate::scheduler::{AcquireEnv, AcquireTarget, acquire};
use crate::source::Funder;
use crate::source_set::{Holder, SourceProvider, SourceSet};
use crate::stop::{ProgressClock, StopPolicy};
use crate::{ClientRangedStore, RangedStore};

/// One blob to fetch and where to write it: the content `hash` (the bao root),
/// its `total_bytes` (authoritative for keying the store and sizing the fetch),
/// the caller-chosen `dest` path the `.partial` and final file are keyed by,
/// and the byte ranges to fill.
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
    /// The `(offset, len)` byte ranges to fill, or `None` for the whole blob.
    /// The fetch promotes the blob only when every byte is present, so a
    /// caller that writes the bytes outside them itself (a bundle entry's
    /// donor splice) finds them in the `.partial` beside `dest`.
    pub ranges: Option<&'a [(u64, u64)]>,
}

/// Fetch content-addressed blobs — a bundle, or a single blob — to files in a
/// directory, through the shared acquire loop ([`crate::acquire`] +
/// [`ClientRangedStore`]) at full throughput.
///
/// A `Downloader` is the download face over the same loop the
/// [`crate::Streamer`] uses: it fetches across a [`SourceSet`] of the injected
/// `holders`, built through the [`SourceProvider`], with up to `max_lanes`
/// lanes and no consumption pacing, then promotes each finished blob to its
/// file. A
/// source that faults cools (in the command-wide [`PeerHealth`]) and returns;
/// its ranges move to the other sources meanwhile. A resumed download (an
/// existing `.partial`, e.g. a bundle layer's chunk-hint dedup) re-pulls only
/// its missing ranges.
///
/// Each target gets a fresh [`SourceSet`], so the provider builds each
/// holder's lane once per target. A [`crate::StaticSources`] hands each lane
/// out once, so a `Downloader` over one fetches one target.
///
/// The faces do not write the buyer store: record each lane's payment after the
/// fetch, whatever its outcome (see the crate docs, and the `download` example
/// for the whole sequence).
///
/// ```no_run
/// use std::path::Path;
///
/// use decdn_client::source::BlobSource;
/// use decdn_client::{DownloadTarget, Downloader, Funder, StaticSources, StreamCandidate};
///
/// async fn download<S: BlobSource, F: Funder>(
///     candidates: Vec<StreamCandidate<S>>,
///     funder: F,
///     hash: [u8; 32],
///     total_bytes: u64,
///     dest: &Path,
/// ) -> anyhow::Result<()> {
///     let downloader = Downloader::new(StaticSources::new(candidates)?, funder);
///     let target = DownloadTarget { hash, total_bytes, dest, ranges: None };
///     downloader.fetch_to_paths(&[target], None).await?;
///     Ok(())
/// }
/// ```
pub struct Downloader<P, F> {
    /// Where the fetch finds holders and builds their lanes.
    provider: P,
    /// The holders every target starts from.
    holders: Vec<Holder>,
    /// The command-wide health every target's sources record into.
    health: Arc<PeerHealth>,
    /// The top-up seam a mid-fetch cap exhaustion funds through.
    funder: F,
    /// The driver's funding/settle policy.
    drive_config: DriveConfig,
    /// The most lanes that stream at once, for every target. Holders past it
    /// wait as reserves, and a holder discovery adds later can use a free
    /// lane.
    max_lanes: usize,
    /// The largest blob accepted, in bytes, or `0` for no cap
    /// ([`Self::max_blob_bytes`]).
    max_blob_bytes: u64,
    /// Targets fetched again from scratch after a finalize hash mismatch
    /// ([`Self::refetched_targets`]).
    refetched: AtomicU64,
}

impl<P, F> std::fmt::Debug for Downloader<P, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Downloader")
            .field("holders", &self.holders.len())
            .field("drive_config", &self.drive_config)
            .finish_non_exhaustive()
    }
}

impl<P, F> Downloader<P, F> {
    /// Build a downloader whose holders and lanes `provider` finds and builds,
    /// funded through `funder`.
    ///
    /// Each target starts by discovering its holders, every holder streams at
    /// once, and reactive top-up is off. The builder methods change each of
    /// these.
    #[must_use]
    pub fn new(provider: P, funder: F) -> Self {
        Self {
            provider,
            holders: Vec::new(),
            health: Arc::default(),
            funder,
            drive_config: DriveConfig::cli(U256::ZERO),
            max_lanes: usize::MAX,
            max_blob_bytes: 0,
            refetched: AtomicU64::new(0),
        }
    }

    /// Start every target from `holders`, in preference order, instead of
    /// discovering them. A probe's measured holders go here.
    #[must_use]
    pub fn holders(mut self, holders: Vec<Holder>) -> Self {
        self.holders = holders;
        self
    }

    /// Stream from at most `max_lanes` holders at once (at least one). The
    /// other holders wait as reserves, and a holder that discovery adds later
    /// can take a free lane.
    #[must_use]
    pub const fn max_lanes(mut self, max_lanes: usize) -> Self {
        self.max_lanes = max_lanes;
        self
    }

    /// Top the pool up through the funder, back to `working_deposit`, when a
    /// fetch runs the deposit low. `U256::ZERO` turns reactive top-up off: a
    /// fetch the deposit cannot cover then fails with
    /// [`crate::NoAffordableSource`].
    #[must_use]
    pub const fn working_deposit(mut self, working_deposit: U256) -> Self {
        self.drive_config.working_deposit = working_deposit;
        self
    }

    /// Record source faults into `health`, which a command shares across its
    /// fetches so a holder that faulted on one stays cooled on the next.
    #[doc(hidden)]
    #[must_use]
    pub fn health(mut self, health: Arc<PeerHealth>) -> Self {
        self.health = health;
        self
    }

    /// Replace the whole funding and settle policy, the working deposit
    /// included.
    #[doc(hidden)]
    #[must_use]
    pub const fn drive_config(mut self, drive_config: DriveConfig) -> Self {
        self.drive_config = drive_config;
        self
    }

    /// How many targets this downloader fetched again from scratch because
    /// their finalized file did not match its hash. Such a pass drops every
    /// byte the store held, including a prefix an earlier run resumed, so a
    /// caller that counts resumed bytes apart from fetched ones reads this to
    /// know the prefix was fetched again.
    #[must_use]
    pub fn refetched_targets(&self) -> u64 {
        self.refetched.load(Ordering::Relaxed)
    }

    /// Cap every target at `max_blob_bytes` (`0` for no cap). A target's size
    /// claim above the cap is clamped to it before its store is sized, and a
    /// blob that holds bytes past the cap fails with [`crate::BlobTooLarge`]
    /// ([`crate::AcquireEnv::max_blob_bytes`]).
    #[must_use]
    pub const fn max_blob_bytes(mut self, max_blob_bytes: u64) -> Self {
        self.max_blob_bytes = max_blob_bytes;
        self
    }
}

impl<P, F> Downloader<P, F>
where
    P: SourceProvider,
    F: Funder,
{
    /// Fetch every `(hash, total_bytes)` entry to a file named by its
    /// content-address hex inside `dir`, returning the written paths in entry
    /// order. See [`Self::fetch_to_paths`].
    ///
    /// `on_progress`, when set, is called with the verified CONTENT progress for
    /// the entry CURRENTLY fetching — `(position, total_bytes)`, both in content
    /// bytes, resetting to that entry's own total at each new entry. A
    /// multi-entry caller drawing one bar accumulates the completed entries'
    /// totals itself; a single-entry download can use it directly.
    ///
    /// # Errors
    ///
    /// Any error [`Self::fetch_to_paths`] returns.
    pub async fn fetch_to_dir(
        &self,
        entries: &[([u8; 32], u64)],
        dir: &Path,
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
    ) -> anyhow::Result<Vec<PathBuf>> {
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
                ranges: None,
            })
            .collect();
        self.fetch_to_paths(&targets, on_progress).await
    }

    /// [`Self::fetch_to_paths_until`] under a stop policy with no limit: the
    /// fetch waits for its sources until the caller drops it.
    ///
    /// # Errors
    ///
    /// Any error [`Self::fetch_to_paths_until`] returns except a give-up.
    pub async fn fetch_to_paths(
        &self,
        targets: &[DownloadTarget<'_>],
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let stop = StopPolicy::new(true, None, Arc::new(ProgressClock::new()));
        self.fetch_to_paths_until(targets, on_progress, &stop).await
    }

    /// Fetch every [`DownloadTarget`] to its own `dest` path, returning the
    /// `dest` of each target it promoted, in target order. A target without
    /// explicit ranges is always promoted, so a whole-blob batch returns every
    /// `dest`; a ranged target that leaves bytes missing is not in the result,
    /// because no file exists at its `dest`. Each blob's `.partial` and final file are
    /// keyed by `dest` (its parent directory and file name), so a resumed
    /// download re-pulls only what it lacks and the promoted file IS `dest` — no
    /// post-finalize rename. The parent directory of each `dest` is created if
    /// missing.
    ///
    /// Each target opens a [`ClientRangedStore`] beside its file and fills its
    /// ranges through [`crate::acquire`] across a fresh [`SourceSet`] of the
    /// holders, then finalizes — promoting `.partial` to `dest`. A target with
    /// explicit [`DownloadTarget::ranges`] that leave bytes missing is not
    /// finalized: its landed ranges stay in the `.partial`. Writes land at
    /// their absolute offsets, so out-of-order and multi-source fills assemble
    /// correctly. The whole blob is fetched at full throughput, with no
    /// read-ahead bound.
    ///
    /// `total_bytes` is the first size claim, a hint: a resumed store's bound
    /// wins over it, and a leg that verifies the final chunk proves the size.
    /// The target `hash` is the bao root every ingested byte is verified
    /// against: a wrong hash surfaces as a verification failure, never as
    /// silent corruption. A finalize whose whole-file hash does not match
    /// (bytes that drifted on disk after they verified) fetches the target
    /// once more.
    ///
    /// `stop` decides when a target that makes no verified progress gives up.
    /// A target that gives up or fails keeps its landed bytes recorded in its
    /// `.partial` for a resume, and is not finalized.
    ///
    /// # Errors
    ///
    /// A `dest` with no file name, a store
    /// open/create/finalize I/O error, a second [`crate::HashMismatch`] at
    /// finalize, or the error the fetch ends a target with: a fatal fault, a
    /// unanimous verdict of the sources, or [`crate::GaveUp`]. The first failing
    /// target aborts the batch; targets already written stay on disk.
    pub async fn fetch_to_paths_until(
        &self,
        targets: &[DownloadTarget<'_>],
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
        stop: &StopPolicy,
    ) -> anyhow::Result<Vec<PathBuf>> {
        self.fetch(targets, None, on_progress, stop).await
    }

    /// [`Self::fetch_to_paths_until`] over a run-wide voucher-ledger registry
    /// (a bundle run's [`LaneLedgers`]). The loop reads and credits every lane
    /// registered across the run for its deposit-solvency view, so concurrent
    /// entries that share one on-chain pool cannot jointly over-draw it.
    ///
    /// # Errors
    ///
    /// Any error [`Self::fetch_to_paths_until`] returns.
    #[doc(hidden)]
    pub async fn fetch_to_paths_shared(
        &self,
        targets: &[DownloadTarget<'_>],
        ledgers: &LaneLedgers,
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
        stop: &StopPolicy,
    ) -> anyhow::Result<Vec<PathBuf>> {
        self.fetch(targets, Some(ledgers), on_progress, stop).await
    }

    /// The fetch behind both entry points. `ledgers` of `None` folds only this
    /// fetch's own lanes, the right view for a solo download.
    async fn fetch(
        &self,
        targets: &[DownloadTarget<'_>],
        ledgers: Option<&LaneLedgers>,
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
        stop: &StopPolicy,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let pacer = BudgetPacer::new();
        let mut written = Vec::with_capacity(targets.len());
        for target in targets {
            let (dir, stem) = dest_store_location(target.dest)?;
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| anyhow::anyhow!("create download dir {}: {e}", dir.display()))?;
            // Off the runtime: opening a finalized blob hashes its final file.
            let store = {
                let (dir, stem) = (dir.clone(), stem.clone());
                let (hash, total) = (target.hash, target.total_bytes);
                // A claim above the cap never sizes the store.
                let total = if self.max_blob_bytes > 0 {
                    total.min(self.max_blob_bytes)
                } else {
                    total
                };
                tokio::task::spawn_blocking(move || {
                    ClientRangedStore::open_or_create(&dir, &stem, hash, total)
                })
                .await
                .map_err(|e| anyhow::anyhow!("open ranged store task: {e}"))?
                .map_err(|e| {
                    anyhow::anyhow!("open ranged store for {}: {e}", target.dest.display())
                })?
            };
            let whole = [(0, target.total_bytes)];
            let ranges = target.ranges.unwrap_or(&whole);
            let mut sources = SourceSet::new(
                &self.provider,
                target.hash,
                Arc::clone(&self.health),
                self.holders.clone(),
            );
            let env = AcquireEnv {
                pacer: &pacer,
                funder: &self.funder,
                drive: &self.drive_config,
                max_lanes: self.max_lanes.max(1),
                stop,
                on_progress,
                ledgers,
                // No consumption pacing: a download runs at full throughput.
                pacing: None,
                max_blob_bytes: self.max_blob_bytes,
            };
            // A finalize whose hash does not match clears every claim and
            // keeps the file, so one more pass fetches the blob again. A
            // second mismatch is a local fault and ends the target.
            let mut passes_left = 2u8;
            loop {
                passes_left = passes_left.saturating_sub(1);
                // A dropped download still records the ranges that landed.
                let flush_on_drop = store.flush_on_drop();
                let fetched = acquire(
                    AcquireTarget {
                        store: &store,
                        hash: target.hash,
                        total_bytes: target.total_bytes,
                        ranges,
                    },
                    &mut sources,
                    &env,
                )
                .await;
                flush_on_drop.disarm();
                fetched?;
                // `acquire` flushes the present record but does not promote; a
                // download keeps the file, so finalize (verify + promote
                // `.partial`) once every byte is present. Explicit ranges can
                // leave bytes to another writer, and the `.partial` then stays
                // for that writer.
                if target.ranges.is_some() && !store.is_complete().await? {
                    break;
                }
                match store.finalize().await {
                    Ok(()) => {
                        written.push(target.dest.to_path_buf());
                        break;
                    }
                    Err(err) if passes_left > 0 && is_hash_mismatch(&err) => {
                        self.refetched.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            hash = %blake3::Hash::from_bytes(target.hash).to_hex(),
                            dest = %target.dest.display(),
                            "the finalized file does not match its hash; fetching it once more"
                        );
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
        Ok(written)
    }
}

/// Whether a finalize failed its whole-file hash ([`crate::HashMismatch`]).
fn is_hash_mismatch(err: &decdn_bao_range::RangedStoreError) -> bool {
    matches!(
        err,
        decdn_bao_range::RangedStoreError::Backend(inner) if inner.is::<crate::HashMismatch>()
    )
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
    use crate::source::{BlobSource, FakeFunder, ScriptedSource};
    use crate::{
        ClientRangedStore, Cumulative, GaveUp, PoolContext, PoolLedger, ProgressClock,
        StaticSources, StopPolicy, StreamCandidate,
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
        Ok(Downloader::new(sources, funder())
            .holders(holders)
            .drive_config(drive_config()))
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
            .drive_config(drive_config())
            .max_lanes(1);
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
                    total_bytes: total,
                    dest: &dest,
                    ranges: None,
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
        ClientRangedStore::seed_checkpointed_prefix(
            dir.path(),
            stem.as_str(),
            &blob,
            seeded_prefix,
        )?;

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
        let paths = downloader.fetch_to_paths(&[target], None).await?;
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
            downloader.fetch_to_paths(&targets, None),
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
            .fetch_to_dir(&[(root, total)], dir.path(), None)
            .await?;
        let path = paths
            .first()
            .ok_or_else(|| anyhow::anyhow!("no path returned"))?;
        anyhow::ensure!(std::fs::read(path)? == blob);
        Ok(())
    }
}
