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
/// use decdn_client::driver::DriveConfig;
/// use decdn_client::source::{BlobSource, Funder};
/// use decdn_client::{DownloadTarget, Downloader, StaticSources, StreamCandidate};
///
/// async fn download<S: BlobSource, F: Funder>(
///     candidates: Vec<StreamCandidate<S>>,
///     funder: F,
///     hash: [u8; 32],
///     total_bytes: u64,
///     dest: &Path,
/// ) -> anyhow::Result<()> {
///     let sources = StaticSources::new(candidates)?;
///     let holders = sources.holders();
///     let lanes = holders.len();
///     let drive = DriveConfig::cli(Default::default());
///     let downloader =
///         Downloader::new(sources, holders, Default::default(), funder, drive, lanes);
///     let target = DownloadTarget { hash, total_bytes, dest, ranges: None };
///     downloader
///         .fetch_to_paths(&[target], None, None)
///         .await?;
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
    /// Build a downloader over `holders`, whose lanes `provider` builds. The
    /// same holders start every target a later `fetch_to_dir` fetches, and
    /// `health` is shared across all of them. With no holder, each target
    /// starts by discovering them. At most `max_lanes` holders (at least one)
    /// stream at once.
    #[must_use]
    pub const fn new(
        provider: P,
        holders: Vec<Holder>,
        health: Arc<PeerHealth>,
        funder: F,
        drive_config: DriveConfig,
        max_lanes: usize,
    ) -> Self {
        Self {
            provider,
            holders,
            health,
            funder,
            drive_config,
            max_lanes,
            max_blob_bytes: 0,
            refetched: AtomicU64::new(0),
        }
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
        self.fetch_to_paths(&targets, None, on_progress).await
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
        ledgers: Option<&LaneLedgers>,
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let stop = StopPolicy::new(true, None, Arc::new(ProgressClock::new()));
        self.fetch_to_paths_until(targets, ledgers, on_progress, &stop)
            .await
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
    /// `ledgers`, when set, is a shared voucher-ledger registry (a bundle run's
    /// `LaneLedgers`): the loop reads and credits EVERY lane registered across
    /// the run for its deposit-solvency view, so concurrent entries sharing one
    /// on-chain pool cannot jointly over-draw it. `None` folds only this fetch's
    /// own lanes, the right view for a solo download.
    ///
    /// `stop` decides when a target that makes no verified progress gives up.
    /// A target that gives up or fails keeps its landed bytes recorded in its
    /// `.partial` for a resume, and is not finalized.
    ///
    /// # Errors
    ///
    /// A `dest` with no file name, a store
    /// open/create/finalize I/O error, a second [`crate::HashMismatch`] at
    /// finalize, or the error [`crate::acquire`] ends a target with: a fatal
    /// fault, a unanimous verdict of the sources, or [`crate::GaveUp`]. The
    /// first failing target aborts the batch; targets already written stay on
    /// disk.
    pub async fn fetch_to_paths_until(
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
mod tests;
