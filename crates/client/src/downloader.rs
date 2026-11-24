//! The `Downloader` consumption face (#1848): fetch a set of content-addressed
//! blobs — a bundle, or a single blob — to files in a directory.
//!
//! A `Downloader` is an OUTPUT + SCHEDULING adapter over the ONE shared
//! acquire loop, not a second engine: each entry opens a [`crate::ClientRangedStore`]
//! in a store beside its file (`<dest>.decdn`) and fills its missing ranges
//! through [`crate::acquire`] across a [`SourceSet`] of the blob's holders —
//! bao-verifying every byte and striping across every holder at full
//! throughput. A source that faults cools and returns; the fetch ends on done,
//! a fatal fault, or the stop policy. It then exports the blob to the final
//! file and removes the store. So a resumed download re-pulls only what it
//! lacks, and a bundle layer above can ask for only the ranges no donor
//! covers.
//!
//! It is the download half of the same loop the `Streamer` (the
//! consumption-paced single-blob face) uses — the Downloader just uncaps the
//! lanes and drops the consumer pacing. Bundles are the main scenario: a bundle
//! is a set of blobs, so both entry points take a slice and write one file per
//! blob. `fetch_to_paths` writes each blob to a caller-chosen destination (a
//! bundle's manifest paths, or a single `decdn get -o` target); `fetch_to_dir`
//! is the convenience over it that names each file by its content-address hex
//! under a directory. A single blob is a bundle of one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::RangedStore;
use crate::credential::{CredentialSlot, RunDemand};
use crate::health::PeerHealth;
use crate::ledgers::LaneLedgers;
use crate::pacer::BudgetPacer;
use crate::ranged_store::{BlobDir, StaleStoreEntry, store_dir_for};
use crate::recovery::RecoveryGate;
use crate::scheduler::{AcquireEnv, AcquireTarget, acquire};
use crate::source::Funder;
use crate::source_set::{Holder, SourceProvider, SourceSet};
use crate::stop::{ProgressClock, StopPolicy};
use decdn_bao_range::RangedStoreError;

/// One blob to fetch and where to write it: the content `hash` (the bao root),
/// its `total_bytes` (authoritative for sizing the store and the fetch), the
/// caller-chosen `dest` path the store and the final file are keyed by, and the
/// byte ranges to fill.
#[derive(Debug, Clone, Copy)]
pub struct DownloadTarget<'a> {
    /// The blob's BLAKE3 content address (its bao root); every ingested byte is
    /// verified against it.
    pub hash: [u8; 32],
    /// The blob's proven content length in bytes.
    pub total_bytes: crate::ProvenSize,
    /// Where to write the finished blob. The store sits beside it, in
    /// `<dest>.decdn`, and the finished file IS `dest`.
    pub dest: &'a Path,
    /// The `(offset, len)` byte ranges to fill, or `None` for the whole blob.
    /// The fetch exports the blob only when every byte is present, so the
    /// ranges of a target that leaves bytes missing stay in its store for
    /// the caller (a bundle entry that assembles the rest from donors).
    pub ranges: Option<&'a [(u64, u64)]>,
}

/// Fetch content-addressed blobs — a bundle, or a single blob — to files in a
/// directory, through the shared acquire loop ([`crate::acquire`] +
/// [`crate::ClientRangedStore`]) at full throughput.
///
/// A `Downloader` is the download face over the same loop the
/// [`crate::Streamer`] uses: it fetches across a [`SourceSet`] of the injected
/// `holders`, built through the [`SourceProvider`], with up to `max_lanes`
/// lanes and no consumption pacing, then exports each finished blob to its
/// file. A source that faults cools (in the command-wide [`PeerHealth`]) and
/// returns; its ranges move to the other sources meanwhile. A resumed download
/// (an existing store beside the file) re-pulls only its missing ranges.
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
/// use decdn_client::{
///     DownloadTarget, Downloader, Funder, ProvenSize, StaticSources, StreamCandidate,
/// };
///
/// // `total_bytes` comes from `check_size_proof` on a node's size proof.
/// async fn download<S: BlobSource, F: Funder>(
///     candidates: Vec<StreamCandidate<S>>,
///     funder: F,
///     hash: [u8; 32],
///     total_bytes: ProvenSize,
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
    /// The funding recovery seam an exhausted candidate set funds through.
    funder: F,
    /// The fetch's funding recovery state, shared by every target.
    recovery: Arc<RecoveryGate>,
    /// A delegated fetch's swappable voucher credential ([`Self::credentials`]).
    credentials: Option<CredentialSlot>,
    /// The most lanes that stream at once, for every target. Holders past it
    /// wait as reserves, and a holder discovery adds later can use a free
    /// lane.
    max_lanes: usize,
    /// The largest blob accepted, in bytes, or `0` for no cap
    /// ([`Self::max_blob_bytes`]).
    max_blob_bytes: u64,
    /// What the caller's run still needs ([`Self::run_demand`]).
    run: Option<Arc<RunDemand>>,
    /// The store every target fetches into, which the caller owns
    /// ([`Self::store`]); `None` keeps a store beside each `dest`.
    shared: Option<BlobDir>,
}

impl<P, F> std::fmt::Debug for Downloader<P, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Downloader")
            .field("holders", &self.holders.len())
            .field("recovery", &self.recovery)
            .finish_non_exhaustive()
    }
}

impl<P, F> Downloader<P, F> {
    /// Build a downloader whose holders and lanes `provider` finds and builds,
    /// funded through `funder`.
    ///
    /// Each target starts by discovering its holders, every holder streams at
    /// once, and the downloader's targets share a fresh [`RecoveryGate`]. The
    /// builder methods change each of these.
    #[must_use]
    pub fn new(provider: P, funder: F) -> Self {
        Self {
            provider,
            holders: Vec::new(),
            health: Arc::default(),
            funder,
            recovery: Arc::default(),
            credentials: None,
            max_lanes: usize::MAX,
            max_blob_bytes: 0,
            run: None,
            shared: None,
        }
    }

    /// Fetch every target into `store`, which the caller opened and removes:
    /// a bundle run keeps one store for all its entries. The downloader
    /// neither stops nor removes it.
    #[must_use]
    pub(crate) fn store(mut self, store: BlobDir) -> Self {
        self.shared = Some(store);
        self
    }

    /// Start every target from `holders`, in preference order, instead of
    /// discovering them. A discovery round's measured holders go here.
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

    /// Run the funding recovery step under `recovery`, the fetch's gate. A
    /// caller that runs its remaining work again after a
    /// [`crate::PoolReplaced`] passes the gate the first pass used, so the
    /// progress rule spans both passes.
    #[must_use]
    pub fn recovery(mut self, recovery: Arc<RecoveryGate>) -> Self {
        self.recovery = recovery;
        self
    }

    /// Pay as a delegate under `slot`'s credential. The provider builds each
    /// lane from [`CredentialSlot::current`]. After a swap, the lanes on the
    /// old key end at a voucher boundary and new lanes resume under the new
    /// key; at the exhausted candidate set the fetch waits for a swap instead
    /// of topping up, and ends with a [`crate::FundingNeeded`].
    #[must_use]
    pub fn credentials(mut self, slot: CredentialSlot) -> Self {
        self.credentials = Some(slot);
        self
    }

    /// Price the delegated funding signal on `run`: the work the caller's
    /// run still plans, when that is more than the targets' own missing
    /// bytes, at the highest quote any lane of the run got. A bundle pull
    /// keeps it at the bundle's undelivered download, so every entry reports
    /// one bundle-wide need.
    #[doc(hidden)]
    #[must_use]
    pub fn run_demand(mut self, run: Arc<RunDemand>) -> Self {
        self.run = Some(run);
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

    /// Cap the bytes held for every target at `max_blob_bytes` (`0` for no
    /// cap). A target's ranges are fetched at any offset while the bytes held
    /// fit; ranges that would hold more fail with [`crate::BlobTooLarge`]
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
        entries: &[([u8; 32], crate::ProvenSize)],
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
    /// `dest` of each target it exported, in target order. A target without
    /// explicit ranges is always exported, so a whole-blob batch returns every
    /// `dest`; a ranged target that leaves bytes missing is not in the result,
    /// because no file exists at its `dest`. The parent directory of each
    /// `dest` is created if missing.
    ///
    /// Each target keeps its store in `<dest>.decdn` beside `dest`, so a
    /// resumed download re-pulls only what it lacks. It fills its ranges
    /// through [`crate::acquire`] across a fresh [`SourceSet`] of the holders,
    /// then exports the blob to `dest` and removes the store. A `dest` that
    /// already holds the blob (an earlier run exported it) is not fetched
    /// again. A target with explicit [`DownloadTarget::ranges`] that leave
    /// bytes missing is not exported: its landed ranges stay in its store.
    /// A downloader given a caller's store fetches every target into that one
    /// instead, and leaves it in place.
    /// Writes land at their absolute offsets, so out-of-order and
    /// multi-source fills assemble correctly. The whole blob is fetched at full
    /// throughput, with no read-ahead bound.
    ///
    /// `total_bytes` is the blob's proven size; the store records it before
    /// the first leg opens. The target `hash` is the bao root every ingested
    /// byte is verified against: a wrong hash surfaces as a verification
    /// failure, never as silent corruption. The exported file is hashed once
    /// more: one whose bytes changed on disk after they verified is removed,
    /// and the target is fetched again from an empty store. A caller's store
    /// then holds a lost export, so the target ends with a stale-store error
    /// instead.
    ///
    /// `stop` decides when a target that makes no verified progress gives up.
    /// A target that gives up or fails keeps its landed bytes in its store for
    /// a resume, and is not exported.
    ///
    /// # Errors
    ///
    /// A `dest` with no file name, a store open, export or removal error, a
    /// second exported file that fails its hash ([`crate::HashMismatch`]), or
    /// the error the fetch ends a target with: a fatal fault, a unanimous
    /// verdict of the sources, or [`crate::GaveUp`]. The first failing target
    /// aborts the batch; targets already written stay on disk.
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
            if self
                .fetch_one(target, ledgers, &pacer, on_progress, stop)
                .await?
            {
                written.push(target.dest.to_path_buf());
            }
        }
        Ok(written)
    }

    /// Fetch one target to its `dest` through its store: the caller's, or one
    /// beside `dest`. Returns whether `dest` holds the blob.
    ///
    /// An exported file that does not hash to the blob holds bytes that
    /// changed on disk after they verified. A store beside `dest` went with
    /// the export, so one more pass fetches the blob again from an empty
    /// store; a second mismatch is a local fault and ends the target. Both
    /// passes share one [`SourceSet`], so the second continues each lane's
    /// voucher sequence. A caller's store keeps a lost export of the blob, so
    /// the target ends with [`StaleStoreEntry`] instead.
    async fn fetch_one(
        &self,
        target: &DownloadTarget<'_>,
        ledgers: Option<&LaneLedgers>,
        pacer: &BudgetPacer,
        on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
        stop: &StopPolicy,
    ) -> anyhow::Result<bool> {
        let own_dir = match self.shared {
            Some(_) => None,
            None => Some(store_dir_for(target.dest)?),
        };
        if let Some(parent) = target.dest.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| anyhow::anyhow!("create download dir {}: {e}", parent.display()))?;
        }
        if dest_holds(target.dest, target.hash).await {
            if let Some(dir) = &own_dir {
                remove_leftover_store(dir).await;
            }
            return Ok(true);
        }
        let mut sources = SourceSet::new(
            &self.provider,
            target.hash,
            Arc::clone(&self.health),
            self.holders.clone(),
        );
        let run = Run {
            ledgers,
            pacer,
            on_progress,
            stop,
        };
        for pass in 0..2u8 {
            match self
                .fetch_pass(own_dir.as_deref(), target, &mut sources, &run)
                .await?
            {
                Pass::Exported => return Ok(true),
                Pass::Kept => return Ok(false),
                Pass::Drifted if own_dir.is_none() => {
                    return Err(anyhow::Error::new(StaleStoreEntry));
                }
                Pass::Drifted if pass == 0 => tracing::warn!(
                    hash = %blake3::Hash::from_bytes(target.hash).to_hex(),
                    dest = %target.dest.display(),
                    "the exported file does not match its hash; fetching it once more"
                ),
                Pass::Drifted => {}
            }
        }
        Err(anyhow::Error::new(crate::HashMismatch))
    }

    /// One pass of [`Self::fetch_one`]: through the store in `own_dir`, which
    /// the pass opens and removes once the blob is exported, or through the
    /// caller's store when `own_dir` is `None`.
    async fn fetch_pass(
        &self,
        own_dir: Option<&Path>,
        target: &DownloadTarget<'_>,
        sources: &mut SourceSet<'_, P>,
        run: &Run<'_>,
    ) -> anyhow::Result<Pass> {
        let Some(store_dir) = own_dir else {
            let dir = self
                .shared
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("a download with no store"))?;
            return Ok(if self.fetch_target(dir, target, sources, run).await? {
                checked_export(target).await
            } else {
                Pass::Kept
            });
        };
        let dir = BlobDir::open(store_dir).await?;
        let exported = match self.fetch_target(&dir, target, sources, run).await {
            Ok(exported) => exported,
            Err(e) => {
                if let Err(shutdown) = dir.shutdown().await {
                    tracing::warn!(error = %shutdown, "store shutdown after a failed fetch");
                }
                return Err(e);
            }
        };
        if !exported {
            dir.shutdown().await?;
            return Ok(Pass::Kept);
        }
        dir.remove().await?;
        Ok(checked_export(target).await)
    }

    /// Fill one target's ranges into its store in `dir` and export it once
    /// every byte is present. Returns whether it exported the blob.
    async fn fetch_target(
        &self,
        dir: &BlobDir,
        target: &DownloadTarget<'_>,
        sources: &mut SourceSet<'_, P>,
        run: &Run<'_>,
    ) -> anyhow::Result<bool> {
        // The whole blob is one tail: it runs to the proven size.
        let whole = [(0, 0)];
        let ranges = target.ranges.unwrap_or(&whole);
        let store = dir
            .blob(
                target.hash,
                target.total_bytes.get(),
                Some(target.dest.to_path_buf()),
            )
            .await?;
        // `dest` does not hold the blob, so a complete entry whose bytes
        // cannot be read is the export that `dest` lost.
        if store.is_lost_export().await {
            return Err(anyhow::Error::new(StaleStoreEntry));
        }
        let env = AcquireEnv {
            pacer: run.pacer,
            funder: &self.funder,
            recovery: &self.recovery,
            credentials: self.credentials.as_ref(),
            max_lanes: self.max_lanes.max(1),
            stop: run.stop,
            on_progress: run.on_progress,
            ledgers: run.ledgers,
            // No consumption pacing: a download runs at full throughput.
            pacing: None,
            max_blob_bytes: self.max_blob_bytes,
            run: self.run.as_deref(),
        };
        // A store whose entry holds every range but is not recorded complete
        // gives back its last chunk group at finalize: one more pass fetches
        // it, and a second refusal ends the target.
        let mut passes_left = 2u8;
        loop {
            passes_left = passes_left.saturating_sub(1);
            acquire(
                AcquireTarget {
                    store: &store,
                    hash: target.hash,
                    total_bytes: target.total_bytes,
                    ranges,
                },
                sources,
                &env,
            )
            .await?;
            // Explicit ranges can leave bytes to another writer, and the store
            // then keeps them for that writer.
            if target.ranges.is_some() && !store.is_complete().await? {
                return Ok(false);
            }
            match store.finalize().await {
                Ok(()) => return Ok(true),
                Err(RangedStoreError::Incomplete) if passes_left > 0 => {
                    tracing::warn!(
                        hash = %blake3::Hash::from_bytes(target.hash).to_hex(),
                        dest = %target.dest.display(),
                        "the store holds every range but not the blob; fetching its last group again"
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}

/// What every pass of one fetch shares.
struct Run<'a> {
    ledgers: Option<&'a LaneLedgers>,
    pacer: &'a BudgetPacer,
    on_progress: Option<&'a (dyn Fn(u64, u64) + Send + Sync + 'a)>,
    stop: &'a StopPolicy,
}

/// How one pass of a target's fetch ended.
enum Pass {
    /// The blob is at `dest`, hashed.
    Exported,
    /// Explicit ranges left bytes missing: the store keeps what landed.
    Kept,
    /// The exported file failed its hash and was removed, with the store.
    Drifted,
}

/// How a pass that exported `target` ends: its `dest` hashes to the blob, or
/// it does not and is removed.
async fn checked_export(target: &DownloadTarget<'_>) -> Pass {
    if dest_holds(target.dest, target.hash).await {
        return Pass::Exported;
    }
    if let Err(e) = tokio::fs::remove_file(target.dest).await {
        tracing::warn!(error = %e, "removing a file that failed its hash");
    }
    Pass::Drifted
}

/// The bytes [`dest_holds`] reads at once: large enough that hashing each
/// read on every core keeps them all busy.
const HASH_READ_BYTES: usize = 16 * 1024 * 1024;

/// Whether `dest` holds the blob `hash`: an earlier run exported it, or this
/// run's export kept its bytes. Hashed off the runtime, in large reads each
/// hashed on every core, so a multi-GB blob costs one fast read; a file that
/// cannot be read does not hold it.
async fn dest_holds(dest: &Path, hash: [u8; 32]) -> bool {
    let dest = dest.to_path_buf();
    tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
        use std::io::Read as _;
        let mut file = match std::fs::File::open(&dest) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; HASH_READ_BYTES];
        loop {
            // Fill the buffer before hashing it, so every `update_rayon`
            // call but the last sees a whole read.
            let mut filled = 0;
            while filled < buf.len() {
                let n = file.read(buf.get_mut(filled..).unwrap_or_default())?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            hasher.update_rayon(buf.get(..filled).unwrap_or_default());
            if filled < buf.len() {
                break;
            }
        }
        Ok(*hasher.finalize().as_bytes() == hash)
    })
    .await
    .is_ok_and(|held| held.unwrap_or(false))
}

/// Best-effort removal of a store an earlier run left after it exported its
/// blob. A store directory left in place is clutter, not a fault.
async fn remove_leftover_store(store_dir: &Path) {
    match tokio::fs::remove_dir_all(store_dir).await {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            tracing::warn!(error = %e, dir = %store_dir.display(), "removing a leftover store");
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
