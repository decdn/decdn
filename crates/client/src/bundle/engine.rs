//! The bundle pull engine behind [`super::BundlePuller`]: fetch every blob a
//! manifest references into an output directory over the paid
//! `cdn/client/v1` path (issue #391).
//!
//! Entries are fetched per *distinct* blob hash. Entries naming the same blob
//! (one file at two paths) are fetched — and paid for — once and hard-linked
//! (or copied) to each path (#1306). Node selection is per blob (#936): with a
//! pinned node every entry is pulled from that one node, otherwise each
//! distinct blob discovers its own holder among the region-nearest active
//! nodes. Every group's whole-blob fetch fans out concurrently, bounded by one
//! global jobs cap (`PullCtx.gate`). Every entry lands beneath the output
//! directory's open handle ([`Landing`]), so a symlink in its path cannot move
//! the write outside the directory.
//!
//! **One shared pool.** The whole bundle pulls from the caller's single
//! `PaymentPool` deposit (ADR 003) — opened once and reused across every
//! provider the manifest touches. Two concurrency guards follow from that: the
//! run's shared `LaneLedgers` (ADR 039) give each `(pool_id, signer, provider)`
//! lane one monotonic voucher issuer, so concurrent fetches sharing a lane —
//! including every leg of a multi-source entry's admitted provider set — draw
//! from the same watermark instead of racing it, while their transfers still
//! run concurrently; and a single global mutex serializes every open-or-reuse
//! call — the pool's on-chain state (deposit, allowance) is one shared resource,
//! regardless of which provider an entry is bound for.
//!
//! **Incremental re-runs.** A run reads `<out_root>/.decdn-manifest.json`
//! before fetching and folds each completed file's record back into it as the
//! pull progresses — rewriting the merged cache atomically whenever a batch of
//! files or a gigabyte of new bytes has landed, and once more when the pull
//! ends. An interrupted pull therefore leaves the files it already landed
//! recorded, so the next run skips them rather than re-hashing the whole tree.
//! Only a file that completed successfully this run is recorded; a still-in-flight
//! or not-yet-fetched entry is never written from bytes this run has not landed.
//! An in-scope path is
//! skipped when the saved record's hash, size, and mtime match the new
//! manifest. A path with no matching saved record is still skipped when
//! re-hashing its on-disk bytes matches the new manifest hash. A path whose
//! on-disk content no longer matches the new manifest hash is re-fetched, not
//! silently kept.
//!
//! **Whole-root content reuse.** A run also reuses a byte-identical file
//! already anywhere in the output root, not only at its own entry path. It
//! consults a whole-root content index built from `.decdn-manifest.json`'s
//! saved records: target blob hash → an on-disk regular file the manifest
//! records with that hash. When a blob's hash hits that index, the candidate
//! is confirmed by a whole-file re-hash, then the destination is materialized
//! by hard link (or copy) with no download and no payment.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::{Context as _, anyhow, bail};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::Deployment;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::payment_pool::PaymentPool;
use futures_util::StreamExt as _;
use iroh::{Endpoint, PublicKey, RelayUrl};
use tokio_util::sync::CancellationToken;

use super::landing::Landing;
use super::run_store::RunStore;
use super::skip_cache::{self, SavedManifest, SavedMtime};
use super::{
    BundleEnd, BundleObserver, BundleOutcome, BundleReport, EntryFailure, EntryLanded, FaultHints,
    FileObserver, MAX_MANIFEST_MB, ManifestEntry, NodeSelection, STAGING_DIR, Selection,
    format_hash, manifest_cache, parse_hash, path_key,
};
use crate::buyer_pool::{EscrowUnconfirmed, OwnedPayer, UnconfirmedEscrows, UntrackedEscrow};
use crate::discovery::{self, NodeCandidate};
use crate::fault::{carries_untracked_escrow, fold_escrow};
use crate::funding::{RunFunding, StalePool, retire_allowed};
use crate::lane_sources::{FirstLeg, LaneSources, discovery_span};
use crate::lanes::{LaneDeps, LanePayer, STALL_WINDOW, SettleOnDrop};
use crate::ordered_writes::OrderedWrites;
use crate::pool_route::PoolPasses;
use crate::ranged_store::{BlobDir, StaleStoreEntry};
use crate::receipt::ReceiptEvidence;
use crate::scheduler::{BUILD_DRAIN, fold_late_escrow};
use crate::session::{ChainConfig, DepositPolicy, DiscoveryOptions, PaymentPoolChain, PullSite};
use crate::stop::Halted;
use crate::stream_cap::LaneStreamCap;
use crate::targets::{
    OpenOpts, OpenRound, OpenSpec, ResolvedTargets, holders_or_none, identity_fresh_candidates,
    now_secs, open_and_order, open_pinned, pinned_targets,
};
use crate::{
    Connections, CredentialSlot, DownloadTarget, Downloader, FundingNeeded, LaneLedgers,
    PeerHealth, PoolReplaced, ProgressCallback, PullDeadlines, RecoveryGate, RunDemand, StopPolicy,
};

/// Group manifest entries by their blob `hash`, preserving first-seen order both
/// across groups and within each group. Entries that name the same blob (one
/// file published at two paths) land in one group so it is fetched once (#1306).
fn group_by_hash<'a>(entries: &[&'a ManifestEntry]) -> Vec<HashGroup<'a>> {
    let mut index: HashMap<[u8; 32], usize> = HashMap::new();
    let mut groups: Vec<HashGroup<'a>> = Vec::new();
    for entry in entries {
        let next = groups.len();
        let at = *index.entry(entry.hash).or_insert(next);
        if at == next {
            groups.push(HashGroup {
                hash: entry.hash,
                entries: vec![*entry],
            });
        } else if let Some(group) = groups.get_mut(at) {
            group.entries.push(*entry);
        }
    }
    groups
}

/// Order hash-groups smallest whole-file first, so small files land early. A
/// group's entries share one blob, so the group's size is any entry's
/// declared `size` (the same OR-across-same-hash-entries rule as
/// [`total_content_bytes`], so a group with one unsized duplicate path still sorts
/// by its real size); a group no entry sizes sorts last. Ties break on the group
/// hash for a deterministic order.
fn order_groups_smallest_first(mut groups: Vec<HashGroup<'_>>) -> Vec<HashGroup<'_>> {
    groups.sort_by(|a, b| {
        let a_size = a.entries.iter().find_map(|e| e.size);
        let b_size = b.entries.iter().find_map(|e| e.size);
        // A group no entry sizes sorts last: map to the max sentinel for the compare.
        let key = |s: Option<u64>| s.unwrap_or(u64::MAX);
        key(a_size)
            .cmp(&key(b_size))
            .then_with(|| a.hash.cmp(&b.hash))
    });
    groups
}

/// Run blocking file work `f` on tokio's blocking pool and return its result.
/// Every group of a bundle pull is a future polled by ONE task, so a blocking
/// call on that task stops every other group's streams from reading or paying —
/// a serving node then ends them when its proof wait faults. `what` names the
/// work in a join error.
async fn off_runtime<T, F>(what: &'static str, f: F) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| anyhow!("{what} task: {e}"))?
}

/// [`Landing::link_or_copy_atomic`] on the blocking pool: its copy fallback
/// moves a whole blob.
async fn link_off_runtime(
    landing: Arc<Landing>,
    src: PathBuf,
    dest: PathBuf,
) -> anyhow::Result<()> {
    off_runtime("link", move || landing.link_or_copy_atomic(&src, &dest)).await
}

/// The result of inspecting the output tree against the new manifest and the
/// saved skip-cache before fetching: which in-scope paths collide, which to
/// skip, and the whole-file donors already on disk.
#[derive(Default)]
struct DiskState {
    /// Entry paths that name the same file as another entry's path on a case-
    /// or normalization-insensitive file system ([`path_key`]). Every one
    /// fails: no entry of the set is the right one to write or skip.
    collides: HashSet<String>,
    /// In-scope manifest paths whose on-disk file already holds an entry's
    /// blob (fast-skip or re-hash-confirmed), each with that blob's hash.
    /// Only the entry whose own hash it is skips ([`DiskState::skips`]).
    skip: HashMap<String, [u8; 32]>,
    /// Whole-file donors already on disk anywhere in the root: target blob hash
    /// → an on-disk regular file the saved manifest records with that hash. A hit
    /// lets a group materialize by link/copy instead of fetching — but only after
    /// the candidate is re-hashed and confirmed (done at the use site).
    whole_file: HashMap<[u8; 32], PathBuf>,
    /// The run's planned work by blob ([`planned_work`]): each group that
    /// still fetches, its planned download, and the prefix an earlier run
    /// left in its store. Filled after the pre-pass.
    work: HashMap<[u8; 32], PlannedWork>,
}

impl DiskState {
    /// Whether the file at `en`'s path already holds `en`'s own blob, so the
    /// entry is skipped. A path confirmed for another entry's blob does not
    /// skip this one.
    fn skips(&self, en: &ManifestEntry) -> bool {
        self.skip.get(&en.path) == Some(&en.hash)
    }
}

/// Every entry path that names the same file as another entry's path
/// ([`path_key`]): each member of a colliding set, not only the later ones.
fn colliding_paths(entries: &[ManifestEntry]) -> HashSet<String> {
    let mut by_key: HashMap<String, Vec<&str>> = HashMap::new();
    for en in entries {
        by_key.entry(path_key(&en.path)).or_default().push(&en.path);
    }
    by_key
        .into_values()
        .filter(|paths| paths.len() > 1)
        .flatten()
        .map(str::to_owned)
        .collect()
}

/// One group's planned download, and the bytes an earlier run already
/// fetched into its store, which a fetch's position counts but this run
/// does not pay for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PlannedWork {
    download: u64,
    resumed: u64,
}

/// The run's planned work after the disk pre-pass, by blob: each hash group
/// that still has a destination to write and no whole-file donor, at its
/// declared size less what an earlier run left in `store`. A group
/// the pre-pass skips or links plans none, so it never counts toward the
/// run's outstanding work while it waits for a jobs slot.
async fn planned_work(
    entries: &[ManifestEntry],
    landing: &Landing,
    overwrite: bool,
    disk: &DiskState,
    store: Option<&BlobDir>,
) -> HashMap<[u8; 32], PlannedWork> {
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let mut work = HashMap::new();
    for group in group_by_hash(&refs) {
        let hash = group.hash;
        let writes = plan_slots(&group.entries, landing, overwrite, disk)
            .iter()
            .any(|slot| matches!(slot, Slot::Write { .. }));
        if !writes || disk.whole_file.contains_key(&hash) {
            continue;
        }
        let download = group.size().unwrap_or(0);
        if download == 0 {
            continue;
        }
        let resumed = span_bytes(&resumed_spans(store, hash).await);
        work.insert(
            hash,
            PlannedWork {
                download: download.saturating_sub(resumed),
                resumed,
            },
        );
    }
    work
}

/// Classify every in-scope entry against the output tree and the saved
/// skip-cache: fast-skip on a matching saved record (hash + size + mtime), else
/// re-hash the on-disk bytes against the new manifest hash, else fetch. With
/// `overwrite` nothing is skipped. The whole-file `hash` is authoritative, so a
/// stale or absent saved record only costs a re-hash, never correctness. An
/// entry whose path collides with another's is never skipped and its file is
/// never a whole-file donor: it fails in [`plan_slots`], with or without
/// `overwrite`.
async fn resolve_disk_state(
    entries: &[ManifestEntry],
    saved: &SavedManifest,
    out_root: &Path,
    overwrite: bool,
) -> DiskState {
    let mut state = DiskState {
        collides: colliding_paths(entries),
        ..DiskState::default()
    };
    if overwrite {
        return state;
    }
    for en in entries {
        if state.collides.contains(&en.path) {
            continue;
        }
        let Ok(dest) = safe_join(out_root, &en.path) else {
            continue; // a bad path fails later in plan_slots; not skippable
        };
        if in_staging(out_root, &dest) {
            continue;
        }
        // `symlink_metadata` does not follow links: a symlink or any
        // non-regular file is never skipped or re-hashed: it is left to the fetch path, whose atomic materialize replaces it
        // with the manifest's regular file. This avoids hashing a symlink target
        // and avoids keeping a non-regular file in place on a fast-skip.
        let Ok(meta) = std::fs::symlink_metadata(&dest) else {
            continue; // absent/unreadable → fetch
        };
        if !meta.is_file() {
            continue; // symlink / dir / non-regular → not skippable, not a donor
        }
        // Fast-skip: the saved record agrees with the file on hash, size, and
        // mtime. The file is accepted by its hash: the manifest's `size` is only
        // declared, and a blob whose true size differs from it is still
        // the entry's blob, so it is not fetched again on every run.
        let fast = saved
            .get(&en.path)
            .is_some_and(|rec| rec.holds(en.hash) && rec.still_matches(&meta));
        if fast {
            state.skip.insert(en.path.clone(), en.hash);
            continue;
        }
        // Re-hash gate: confirm the on-disk bytes against the new manifest hash.
        let dest_buf = dest.clone();
        let got = tokio::task::spawn_blocking(move || hash_file(&dest_buf)).await;
        if let Ok(Ok(got)) = got
            && got == en.hash
        {
            state.skip.insert(en.path.clone(), en.hash);
        }
    }
    // Widen reuse to the whole root: every saved record whose file still exists
    // as a regular file is a whole-file donor keyed by content, for a
    // no-download link verified by re-hash at the use site. This is what lets a
    // file shared across bundles at different paths be reused.
    //
    // A path this run may overwrite (an in-scope entry that does not skip,
    // one that fails on a collision included) is no donor: its bytes can be
    // atomically replaced by its own group between the whole-file link's
    // re-hash and its link/copy (TOCTOU), which would silently corrupt the
    // link's destination with no post-link verification to catch it. Paths
    // compare by [`path_key`], so a record at `readme` is no donor while this
    // run writes `README`: on a case-insensitive file system the two are one
    // file.
    let will_write: HashSet<String> = entries
        .iter()
        .filter(|e| !state.skips(e))
        .map(|e| path_key(&e.path))
        .collect();
    for (path, rec) in saved.records() {
        let Ok(src) = safe_join(out_root, path) else {
            continue;
        };
        if in_staging(out_root, &src) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&src) else {
            continue; // gone / unreadable
        };
        if !meta.is_file() {
            continue; // symlink / non-regular → never a donor
        }
        if !will_write.contains(&path_key(path))
            && let Ok(h) = parse_hash(&rec.hash)
        {
            state.whole_file.entry(h).or_insert(src);
        }
    }
    state
}

/// Where an entry at `path` lands under `out_root`: the § Path-safety join,
/// outside the paths the pull keeps its own state in.
///
/// # Errors
///
/// `path` escapes `out_root`, or lands in the staging directory, on the
/// skip-cache file, or in the manifest cache.
pub(super) fn writable_dest(out_root: &Path, path: &str) -> anyhow::Result<PathBuf> {
    let dest = safe_join(out_root, path)?;
    // Reserve the staging dir: an entry resolving inside
    // `<out_root>/.decdn-partial/` would collide with a per-hash staging
    // file, and `remove_staging` could then delete a materialized output.
    anyhow::ensure!(
        !in_staging(out_root, &dest),
        "manifest path {path:?} is inside the reserved staging directory {STAGING_DIR}/"
    );
    // Reserve the pull's own state: an entry at the skip-cache file would be
    // overwritten by the run's skip-cache flush, and an entry inside the
    // manifest cache would collide with a cached manifest.
    let is_skip_cache = reserved_under(out_root, &dest, skip_cache::SAVED_MANIFEST_NAME)
        .is_some_and(|mut rest| rest.next().is_none());
    anyhow::ensure!(
        !is_skip_cache && reserved_under(out_root, &dest, manifest_cache::CACHE_DIR).is_none(),
        "manifest path {path:?} is reserved for the pull's own state ({} or {}/)",
        skip_cache::SAVED_MANIFEST_NAME,
        manifest_cache::CACHE_DIR,
    );
    Ok(dest)
}

/// The components of `dest` past `out_root/<name>`, when `dest` is that path
/// or lies inside it; `None` otherwise. The component under `out_root`
/// compares by [`path_key`], so on a file system that ignores case or Unicode
/// normalization every spelling of a reserved name is reserved too.
fn reserved_under<'a>(
    out_root: &Path,
    dest: &'a Path,
    name: &str,
) -> Option<std::path::Components<'a>> {
    let mut rest = dest.strip_prefix(out_root).ok()?.components();
    let first = rest.next()?.as_os_str().to_str()?;
    (path_key(first) == path_key(name)).then_some(rest)
}

/// Whether `dest` is the staging directory under `out_root` or lies inside it
/// ([`reserved_under`]).
fn in_staging(out_root: &Path, dest: &Path) -> bool {
    reserved_under(out_root, dest, STAGING_DIR).is_some()
}

/// Resolve each entry's on-disk destination and classify it — a resolve failure,
/// a path to skip, or a path to write — before any fetch. `disk` is the
/// [`resolve_disk_state`] pre-pass result: an entry skips when its own hash is
/// confirmed at its path by a matching saved-manifest record (fast-skip) or,
/// failing that, by a re-hash of the on-disk bytes. A path whose content
/// changed is written, not silently kept. Evaluated **per destination**, so
/// one path of a duplicated blob can be skipped while another is written.
/// With `overwrite` set, nothing skips and every destination is written. A
/// path that collides with another entry's ([`DiskState::collides`]) fails,
/// overwrite or not, and so does a path to write whose parent chain already
/// cannot take it ([`Landing::check`]), both before a fetch is paid for. A
/// path to skip is not checked: nothing is written to it.
fn plan_slots<'a>(
    entries: &[&'a ManifestEntry],
    landing: &Landing,
    overwrite: bool,
    disk: &DiskState,
) -> Vec<Slot<'a>> {
    entries
        .iter()
        .map(|en| match writable_dest(landing.root(), &en.path) {
            Err(e) => Slot::Failed(EntryOutcome::failed(&en.path, &e)),
            Ok(_) if disk.collides.contains(&en.path) => Slot::Failed(EntryOutcome::failed(
                &en.path,
                &anyhow!(
                    "another entry names the same file on a case- or normalization-insensitive \
                     file system: {:?}",
                    en.path
                ),
            )),
            Ok(_) if !overwrite && disk.skips(en) => Slot::Skip,
            Ok(dest) => match landing.check(&dest) {
                Ok(()) => Slot::Write {
                    label: en.path.as_str(),
                    dest,
                },
                Err(e) => Slot::Failed(EntryOutcome::failed(&en.path, &e)),
            },
        })
        .collect()
}

/// Turn classified [`Slot`]s into outcomes: materialize the blob at the first
/// writable destination and hard-link/copy it to the rest. `materialize` is the
/// paid path and runs **at most once** per group — every later destination goes
/// through the free `link`, reported as [`EntryOutcome::Linked`] so the summary
/// never implies a second paid fetch. If the first materialize fails, the next
/// writable path retries it from the same already-fetched staging file (no
/// re-fetch), so one bad path can't doom the group.
///
/// Parameterized over the two operations so the fetch-once / link-rest invariant
/// — the core of #1306 — is unit-testable without a live endpoint or pool. Both
/// are futures: a copy of a multi-GB blob must run off the runtime (see
/// [`off_runtime`]), because every group of a run shares one task.
async fn materialize_group<MF, MFut, LF, LFut>(
    slots: Vec<Slot<'_>>,
    mut materialize: MF,
    mut link: LF,
) -> Vec<EntryOutcome>
where
    MF: FnMut(PathBuf) -> MFut,
    MFut: std::future::Future<Output = anyhow::Result<u64>>,
    LF: FnMut(PathBuf, PathBuf) -> LFut,
    LFut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut canonical: Option<PathBuf> = None;
    let mut outcomes = Vec::with_capacity(slots.len());
    for slot in slots {
        let outcome = match slot {
            Slot::Failed(o) => o,
            Slot::Skip => EntryOutcome::Skipped,
            Slot::Write { label, dest } => match &canonical {
                Some(src) => match link(src.clone(), dest).await {
                    Ok(()) => EntryOutcome::Linked,
                    Err(e) => EntryOutcome::failed(label, &e),
                },
                None => match materialize(dest.clone()).await {
                    Ok(n) => {
                        canonical = Some(dest);
                        EntryOutcome::Fetched(n)
                    }
                    Err(e) => EntryOutcome::failed(label, &e),
                },
            },
        };
        outcomes.push(outcome);
    }
    outcomes
}

/// Re-hash an on-disk whole-file `donor` candidate against `hash` on the
/// blocking pool and, on a match, return its on-disk length — the authoritative
/// size of what is about to be linked, rather than the manifest's optional
/// `size` (absent on the wire for some bundles). `None` covers a hash mismatch,
/// a read or metadata failure, and a failed join.
async fn verified_donor_len(donor: &Path, hash: [u8; 32]) -> Option<u64> {
    let target = donor.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let got = hash_file(&target).ok()?;
        (got == hash)
            .then(|| std::fs::metadata(&target).ok())
            .flatten()
            .map(|m| m.len())
    })
    .await
    .unwrap_or(None)
}

/// Materialize every write slot from an on-disk whole-file `donor` (a byte-
/// identical file already in the root), by link/copy — no fetch, no payment.
/// The first write is [`EntryOutcome::Deduped`] (the reused blob, counted once);
/// each further duplicate path is [`EntryOutcome::Linked`], mirroring the
/// fetch-once/link-rest accounting of #1306. The donor is verified by the
/// caller (a re-hash against the target hash) before this is called.
async fn materialize_from_donor(
    landing: &Arc<Landing>,
    slots: Vec<Slot<'_>>,
    donor: &Path,
    size: u64,
) -> Vec<EntryOutcome> {
    materialize_group(
        slots,
        |dest| {
            let donor = donor.to_path_buf();
            let landing = landing.clone();
            async move { link_off_runtime(landing, donor, dest).await.map(|()| size) }
        },
        |src, dest| link_off_runtime(landing.clone(), src, dest),
    )
    .await
    .into_iter()
    .map(|o| match o {
        // materialize_group tags the first materialize Fetched(n); relabel it
        // Deduped since no payment or download occurred.
        EntryOutcome::Fetched(n) => EntryOutcome::Deduped(n),
        other => other,
    })
    .collect()
}

/// The run's whole-download content size — the fixed denominator for the total
/// progress bar. Every entry declares its whole-file `size` and is deduped by
/// `hash` (a blob fetched once and materialized to several paths counts once,
/// matching the fetch-once grouping). A blob whose manifest `size` is absent contributes nothing,
/// exactly as it then moves the total bar not at all, so the numerator and
/// denominator stay consistent. `None` when nothing kept declares a size — the
/// total bar is then omitted and only per-file bars render.
fn total_content_bytes(entries: &[ManifestEntry]) -> Option<u64> {
    // Entries keyed by whole-file hash, OR-ing in a declared size wherever one of
    // the same-hash entries carries it (the file bar picks its size the same way).
    let mut by_hash: HashMap<[u8; 32], Option<u64>> = HashMap::new();
    for entry in entries {
        let slot = by_hash.entry(entry.hash).or_insert(None);
        *slot = slot.or(entry.size);
    }
    let mut sum: u64 = 0;
    let mut any_sized = false;
    for size in by_hash.values().flatten() {
        sum = sum.saturating_add(*size);
        any_sized = true;
    }
    any_sized.then_some(sum)
}

/// Each distinct blob of `entries` with its declared size and the wire bytes
/// a fresh pull of it pays for.
pub(super) fn planned_downloads(entries: &[ManifestEntry]) -> Vec<super::PlannedBlob> {
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    group_by_hash(&refs)
        .iter()
        .map(|group| super::PlannedBlob {
            hash: group.hash,
            size: group.size(),
            wire: blob_wire(group),
        })
        .collect()
}

/// The wire bytes a pull of one whole blob pays for: its content with the bao
/// proof tree (ADR 038 § Payment metering). `0` when the blob declares no
/// size.
fn blob_wire(group: &HashGroup<'_>) -> u64 {
    let Some(total) = group.size() else {
        return 0;
    };
    decdn_bao_range::align_range_clamped(0, total, total)
        .map_or(total, |aligned| aligned.wire_len())
}

/// A group of manifest entries that all name the same blob `hash` — one file
/// published at two (or more) bundle paths. Non-empty by construction; every
/// member's `hash` equals `hash`.
#[derive(Clone)]
struct HashGroup<'a> {
    hash: [u8; 32],
    entries: Vec<&'a ManifestEntry>,
}

impl HashGroup<'_> {
    /// The blob's declared size: the first `size` any of its entries declares.
    fn size(&self) -> Option<u64> {
        self.entries.iter().find_map(|e| e.size)
    }
}

/// Per-destination classification within a hash-group, resolved before any
/// fetch: a resolve failure, an already-present file to skip, or a path to
/// write. `label` (the entry's manifest-relative path) is retained only to tag
/// a later failure.
#[derive(Debug)]
enum Slot<'a> {
    Failed(EntryOutcome),
    Skip,
    Write { label: &'a str, dest: PathBuf },
}

/// Per-entry result, kept (never short-circuited) so one failure doesn't abort
/// the others — re-running resumes via skip-existing. `Linked` is a duplicate
/// destination materialized from an already-fetched sibling (hard link or copy),
/// kept distinct from `Fetched` (a paid network pull) so the summary never
/// implies a blob was paid for twice — the whole point of #1306.
#[derive(Clone, Debug)]
enum EntryOutcome {
    Fetched(u64),
    Linked,
    Skipped,
    /// A destination materialized from an on-disk whole-file donor (a byte-
    /// identical file already in the root, verified by re-hash) — no download,
    /// no payment. The `u64` is the file's size, counted as reused, never
    /// downloaded.
    Deduped(u64),
    Failed {
        path: String,
        err: String,
    },
    /// A destination the pull did not run: a sibling's halt left it unwritten
    /// ([`Halted`]), or its group never settled ([`SettledGroup::not_run`]).
    /// The run summary counts it as not run, and the skip-cache does not
    /// record it.
    NotRun,
}

/// One hash-group's result.
struct GroupRun {
    /// One outcome per entry in the group, in order.
    outcomes: Vec<EntryOutcome>,
    /// The content bytes the group paid for and resumed, when its blob fetch
    /// landed ([`PullCtx::pull_entry_untimed`]). `None` when it fetched nothing.
    bytes: Option<EntryBytes>,
    /// The fault that ends the whole pull ([`ends_the_pull`]), when the
    /// group's blob fetch failed with one.
    stop: Option<anyhow::Error>,
}

impl GroupRun {
    /// A result that fetched nothing.
    const fn done(outcomes: Vec<EntryOutcome>) -> Self {
        Self {
            outcomes,
            bytes: None,
            stop: None,
        }
    }

    /// The group's blob fetch landed with `bytes`, and `outcomes` is how each
    /// destination then materialized.
    const fn landed(outcomes: Vec<EntryOutcome>, bytes: EntryBytes) -> Self {
        Self {
            outcomes,
            bytes: Some(bytes),
            stop: None,
        }
    }

    /// The group's blob fetch failed with `err`: every writable slot fails,
    /// and `err` ends the whole pull when [`ends_the_pull`] says so. A sibling
    /// that ended the pull before this fetch finished halts it ([`Halted`]):
    /// every writable slot is then [`EntryOutcome::NotRun`], and the halt ends
    /// nothing.
    fn fetch_failed(slots: Vec<Slot<'_>>, err: anyhow::Error) -> Self {
        if err.downcast_ref::<Halted>().is_some() {
            return Self::done(not_run_all(slots));
        }
        Self {
            outcomes: fail_all(slots, &err),
            bytes: None,
            stop: ends_the_pull(&err).then_some(err),
        }
    }
}

/// What a failed entry fetch stops (ADR 039 § Failure handling: reassign-only
/// tail): a fault
/// only the user can fix stops every entry. Every other fault belongs to its
/// entry alone.
fn entry_scope(err: &anyhow::Error) -> crate::FatalScope {
    match crate::classify(err) {
        crate::Fault::Fatal(scope) => scope,
        _ => crate::FatalScope::Item,
    }
}

/// Whether a failed entry fetch ends the whole pull: a command-scope fault
/// ([`entry_scope`]), or a give-up. A give-up is item-scoped, but every entry
/// shares the command's progress clock, so every other entry gives up at the
/// same moment.
fn ends_the_pull(err: &anyhow::Error) -> bool {
    entry_scope(err) == crate::FatalScope::Command || err.downcast_ref::<crate::GaveUp>().is_some()
}

/// A pull's byte accounting: `downloaded` is the content bytes this run fetched and
/// paid for across the distinct blobs fetched (a blob materialized to several paths
/// counts once, #1306); `reconstructed` is the total bytes written to disk (every
/// materialized copy); `resumed` is the content bytes that earlier runs fetched
/// into entry stores and this run resumed. `downloaded` is content bytes, not
/// exact on-wire bytes: it omits bao proof overhead.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Transfer {
    downloaded: u64,
    reconstructed: u64,
    resumed: u64,
}

impl Transfer {
    /// Combine two tallies (saturating — a pull never reports a wrapped total).
    const fn add(self, other: Transfer) -> Transfer {
        Transfer {
            downloaded: self.downloaded.saturating_add(other.downloaded),
            reconstructed: self.reconstructed.saturating_add(other.reconstructed),
            resumed: self.resumed.saturating_add(other.resumed),
        }
    }
}

/// The run's candidate list, every active registered node, and where it came
/// from. Each entry draws its own sample from the whole list
/// ([`Registry::entry_candidates`]), so no registered node is hidden from
/// every entry by one draw.
#[derive(Debug, Clone)]
pub(super) struct Registry {
    /// Every active registered node.
    pub(super) nodes: Vec<NodeCandidate>,
    /// The list is the registry-outage fallback
    /// ([`discovery::Bootstrap::Cached`]), which the region allowlist leaves
    /// alone: the run is already down to what the store kept. A live registry
    /// read, or the peer store's identity-fresh records that stand in for
    /// one, is narrowed.
    pub(super) outage_fallback: bool,
}

impl Registry {
    /// One entry's discovery candidates: a fresh sample of the list,
    /// same-region nodes first, kept to `site.region_allowlist` with the
    /// widening rule a single fetch applies
    /// ([`crate::targets::select_with_widening`]). A cached-outage list ignores
    /// the allowlist. A sample per entry costs no extra open, because each
    /// entry opens its own candidates, and a node that one draw leaves out can
    /// still reach other entries.
    pub(super) fn entry_candidates(&self, site: &PullSite) -> Vec<NodeCandidate> {
        self.allowed(site, discovery::SELECT_K)
    }

    /// At most `k` of the list, same-region nodes first, kept to
    /// `site.region_allowlist` with the widening rule
    /// ([`crate::targets::select_up_to`]); a cached-outage list ignores the
    /// allowlist.
    pub(super) fn allowed(&self, site: &PullSite, k: usize) -> Vec<NodeCandidate> {
        let allow: &[decdn_protocol::Region] = if self.outage_fallback {
            &[]
        } else {
            site.region_allowlist.as_slice()
        };
        crate::targets::select_up_to(
            self.nodes.clone(),
            site.region.as_deref(),
            allow,
            &crate::StoreConfig::default(),
            k,
        )
    }
}

/// The rates of `nodes`, nearest first: each node's `(rtt_ms, rate_per_mb)`
/// sample from this run, else its peer-store rate and latency
/// ([`crate::quote::store_rate`]). A node with no known rate, or whose last
/// failure still suppresses it, is left out; one with no known RTT sorts
/// last.
pub(super) fn rate_hints_of(
    nodes: &[(PublicKey, Address)],
    samples: &HashMap<PublicKey, (f64, u64)>,
    store: &crate::PeerStore,
    now_secs: u64,
) -> Vec<crate::quote::RateHint> {
    let mut known: Vec<(f64, crate::quote::RateHint)> = nodes
        .iter()
        .filter_map(|&(node_id, provider)| {
            let (rtt_ms, rate_per_mb) = samples
                .get(&node_id)
                .copied()
                .or_else(|| crate::quote::store_rate(store, &node_id, now_secs))?;
            Some((
                rtt_ms,
                crate::quote::RateHint {
                    provider,
                    rate_per_mb,
                },
            ))
        })
        .collect();
    known.sort_by(|a, b| a.0.total_cmp(&b.0));
    known.into_iter().map(|(_, hint)| hint).collect()
}

/// The run's [`Registry`]. The peer store's identity-fresh records serve as
/// the list when enough of them qualify ([`cached_registry`]), and the run
/// issues no `getRegisteredNodes`. Otherwise the registry is read once. A
/// cached list that runs out of holders falls back to a live read through the
/// in-run rediscovery ([`LaneSources`]).
pub(super) async fn discover_candidates(
    chain: &ChainConfig,
    site: &PullSite,
    discovery: &DiscoveryOptions,
) -> anyhow::Result<Registry> {
    let capacity_bond = chain.governance.capacity_bond;
    if !discovery.rediscover
        && let Some(cached) = cached_registry(&site.data_dir, now_secs())
    {
        return Ok(Registry {
            nodes: cached,
            outage_fallback: false,
        });
    }
    let bootstrap = crate::discovery::bootstrap_nodes(
        &chain.governance.rpc_url,
        capacity_bond,
        &site.data_dir,
        discovery.registry_timeout,
    )
    .await?;
    // See `crate::targets`' discovery: the degraded bootstrap paths reach the
    // operator through the return value; surface it here at `warn`.
    if let Some(warning) = bootstrap.warning() {
        tracing::warn!("{warning}");
    }
    let outage_fallback = matches!(bootstrap, discovery::Bootstrap::Cached { .. });
    let all = bootstrap.into_peers();
    if all.is_empty() {
        bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    Ok(Registry {
        nodes: all,
        outage_fallback,
    })
}

/// The peer store's identity-fresh records under `data_dir`, or `None` when
/// fewer than [`crate::StoreConfig::min_fresh_candidates`] qualify. The
/// records are the last live registry read, confirmed within
/// [`crate::StoreConfig::identity_refresh_secs`]. The run only reads
/// them: identity refreshes on a live registry read alone, so the refresh
/// horizon still expires.
fn cached_registry(data_dir: &Path, now_secs: u64) -> Option<Vec<NodeCandidate>> {
    let store = crate::PeerStore::open(data_dir);
    let cfg = crate::StoreConfig::default();
    let cached = identity_fresh_candidates(&store, &cfg, now_secs);
    (cached.len() >= cfg.min_fresh_candidates).then_some(cached)
}

/// How often the pull's poll watch ([`warn_when_unpolled`]) ticks.
const POLL_WATCH_TICK: std::time::Duration = std::time::Duration::from_secs(1);

/// How late a poll-watch tick may fire before the pull logs that its task was
/// not polled.
const POLL_WATCH_LATE: std::time::Duration = std::time::Duration::from_secs(2);

/// Run `drive` beside `flush` until both finish, or until `cancel` fires.
/// Returns whether `cancel` stopped it.
///
/// The cancel drops `drive`: every in-flight fetch stops where it is (its drop
/// guard queues what it paid for recording, and its store is the next run's
/// resume prefix). The flush channel's sender drops with `drive`, so
/// `flush` still writes every group that settled before it returns. A poll
/// watch ([`warn_when_unpolled`]) runs beside `drive` until `drive` ends or
/// `cancel` stops it.
async fn drive_until_interrupted(
    drive: impl std::future::Future<Output = ()>,
    flush: impl std::future::Future<Output = ()>,
    cancel: &CancellationToken,
) -> bool {
    let drive = async {
        tokio::select! {
            () = drive => false,
            () = cancel.cancelled() => true,
            never = warn_when_unpolled() => match never {},
        }
    };
    tokio::join!(drive, flush).0
}

/// Log a WARN each time a tick fires more than [`POLL_WATCH_LATE`] late, that
/// is, each time the task that polls this future was not polled for a while.
/// Never returns.
///
/// Every entry and lane of a pull runs on one task, so anything that blocks
/// that task stops every lane at once, and the nodes see the lanes' streams
/// go quiet together (#2211). A tick's lateness is a lower bound on that
/// freeze: a freeze that starts just after a tick misses up to one
/// [`POLL_WATCH_TICK`], so only a freeze longer than about 3 s is sure to be
/// logged. A system suspend shows up the same way.
async fn warn_when_unpolled() -> std::convert::Infallible {
    let mut tick = tokio::time::interval(POLL_WATCH_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let due = tick.tick().await;
        let late = due.elapsed();
        if late > POLL_WATCH_LATE {
            tracing::warn!(
                late_ms = late.as_millis(),
                "the bundle task was not polled for at least {late:?}: a blocking call on the \
                 pull task froze every lane (or the system was suspended); run with -vv to see \
                 which"
            );
        }
    }
}

/// What [`PullCtx::pull_all`] hands back: every entry's outcome, the run's
/// byte tally, whether a cancel stopped it, and the fault that ended it
/// early. An entry that a run that stopped early did not settle is
/// [`EntryOutcome::NotRun`].
struct PullRun {
    outcomes: Vec<EntryOutcome>,
    /// One line per landed entry whose blob differs in size from its
    /// manifest ([`size_warnings`]).
    warnings: Vec<String>,
    transfer: Transfer,
    interrupted: bool,
    /// The fault that ended the whole pull ([`ends_the_pull`]), if one did.
    stopped: Option<anyhow::Error>,
}

impl PullRun {
    /// A run `fault` stopped before any group started: every entry not run.
    fn stopped_before_start(entries: usize, fault: anyhow::Error) -> Self {
        Self {
            outcomes: std::iter::repeat_with(|| EntryOutcome::NotRun)
                .take(entries)
                .collect(),
            warnings: Vec::new(),
            transfer: Transfer::default(),
            interrupted: false,
            stopped: Some(fault),
        }
    }
}

/// The pool replacement that ended `run`, when a funding recovery step opened
/// a new pool and the pass was not interrupted: the one the rest runs again
/// against. Any other replacement runs nothing again: a sibling entry's step
/// that replaced the pool while another entry's fault ended the pass, one
/// that an untracked escrow ended the pass over (it survives in the error
/// only as text), or one in an interrupted pass. The gate lists each of them
/// ([`RecoveryGate::replacements`]).
fn replaced_pool(run: &PullRun) -> Option<PoolReplaced> {
    if run.interrupted {
        return None;
    }
    run.stopped
        .as_ref()
        .and_then(|err| err.downcast_ref::<PoolReplaced>())
        .copied()
}

/// Start a pass against the pool a funding recovery step opened (ADR 003 §
/// Funding recovery). Lanes key on the pool, so the pass starts with fresh
/// `ledgers` and `funding` facts on the same escrow handle: the funding
/// recovery step tops up the pool the run's first lane names
/// ([`RunFunding::note_pool`]), and a run that kept the closed pool there
/// would open yet another pool instead of funding the new one. The gate
/// steps on the new pool ([`RecoveryGate::start_next_pass`]).
fn start_pass_on_new_pool(
    recovery: &RecoveryGate,
    ledgers: &mut LaneLedgers,
    funding: &mut RunFunding,
) {
    *ledgers = LaneLedgers::new();
    *funding = RunFunding::reporting_escrows_to(funding.escrows().clone());
    recovery.start_next_pass();
}

/// Start a pull clear of a replacement the gate holds from before it: a
/// manifest fetch's, or an earlier pull's that ran nothing again or came in
/// its second pass. Its new pool is the one this pull's lanes reuse, so the
/// pull starts as a pass against that pool ([`start_pass_on_new_pool`]). Left
/// on the gate, it would answer the pull's first recovery step with no step.
/// The gate still lists it ([`RecoveryGate::replacements`]).
fn leave_carried_replacement(
    recovery: &RecoveryGate,
    ledgers: &mut LaneLedgers,
    funding: &mut RunFunding,
) {
    if recovery.leave_carried().is_some() {
        start_pass_on_new_pool(recovery, ledgers, funding);
    }
}

/// `err`, the fault that ended a manifest fetch, with what comes next. A
/// manifest fetch runs nothing again against a pool a recovery step opened:
/// the pull that follows it pays from that pool.
fn manifest_fetch_error(err: anyhow::Error) -> anyhow::Error {
    let err = match err.downcast_ref::<PoolReplaced>().copied() {
        Some(replaced) => err.context(format!(
            "pool {} is now the current pool; run the pull again to fetch from it",
            replaced.opened
        )),
        None => err,
    };
    let err = if err.downcast_ref::<crate::BlobTooLarge>().is_some() {
        err.context(format!(
            "the blob is larger than a bundle manifest can be ({MAX_MANIFEST_MB} MiB, or the \
             run's blob cap when that is lower); drop the trailing path to fetch the raw blob"
        ))
    } else {
        err
    };
    err.context("fetch bundle manifest blob")
}

/// Whether a pull may retire a stale stored pool
/// ([`RunFunding::retire_stale_pool`]): it `failed` ([`leaves_work`], so an
/// interrupted pull never does), paid for no byte, and no pool replacement
/// reran it, since that rerun pays from a pool the run opened.
fn retires_stale_pools(
    failed: bool,
    paid: alloy::primitives::U256,
    rerun: Option<PoolReplaced>,
) -> bool {
    failed && paid.is_zero() && rerun.is_none()
}

/// `stopped`, the fault that ended a failed pull, with each `stale` pool as
/// context on it. A pull that no fault stopped takes each one as a warning
/// instead, so the report names it.
fn fold_stale_pools(
    mut stopped: Option<anyhow::Error>,
    stale: Vec<StalePool>,
    warnings: &mut Vec<String>,
) -> Option<anyhow::Error> {
    for note in stale {
        if let Some(err) = stopped {
            stopped = Some(err.context(note));
        } else {
            warnings.push(note.to_string());
        }
    }
    stopped
}

/// How a pull that ended with `stopped` ends, once every pass ran.
/// `rerun` is the pool replacement a second pass ran against, `pending` the
/// `openPool` and `topUp` transactions the puller's handle holds with no
/// receipt read ([`crate::buyer_pool::UnconfirmedEscrows::pending`]), and
/// `untracked` the escrows it holds with no local record crediting them
/// ([`crate::buyer_pool::UnconfirmedEscrows::untracked`]).
///
/// An interrupted pull ends [`BundleEnd::Interrupted`], which carries no
/// error: its caller names what the handle holds. Otherwise each escrow that
/// `stopped` does not already name is folded into it: the untracked ones,
/// then the pending ones, the first as the error's root
/// ([`crate::buyer_pool::EscrowUntracked`], [`crate::buyer_pool::OpenUnconfirmed`]
/// or [`crate::buyer_pool::TopUpUnconfirmed`]). An entry dropped before it
/// reported its escrow, an entry dropped while its receipt read was pending,
/// or an earlier pull on the handle, leaves that escrow named nowhere else.
/// The escrow wins over any outcome, as a late lane build's does for one
/// fetch: a pull that completed stops, and the stop survives only as text, so
/// a [`crate::GaveUp`], a [`PoolReplaced`] or a [`FundingNeeded`] cannot send
/// the caller into a rerun that escrows again. A second replacement in one
/// run ends the pull "funding needed".
fn pull_end(
    interrupted: bool,
    stopped: Option<anyhow::Error>,
    rerun: Option<PoolReplaced>,
    pending: Vec<EscrowUnconfirmed>,
    untracked: Vec<UntrackedEscrow>,
) -> BundleEnd {
    if interrupted {
        return BundleEnd::Interrupted;
    }
    // An error that invites a rerun never names an escrow: its text does not
    // stop the caller from rerunning.
    let named = stopped
        .as_ref()
        .filter(|err| !invites_rerun(err))
        .map(|err| format!("{err:#}"));
    let unnamed = untracked.into_iter().filter(|held| {
        named
            .as_ref()
            .is_none_or(|named| !named.contains(&held.tx.to_string()))
    });
    // The oldest escrow is the root, and each later one is context on it.
    let escrows = unnamed.fold(None, |escrows: Option<anyhow::Error>, held| {
        Some(match escrows {
            None => held.into_error(),
            Some(err) => err.context(format!(
                "another openPool or topUp of this command also escrowed USDC no record \
                 credits: {held}"
            )),
        })
    });
    let unnamed = pending.into_iter().filter(|escrow| {
        named
            .as_ref()
            .is_none_or(|named| !named.contains(&escrow.to_string()))
    });
    let escrows = unnamed.fold(escrows, |escrows, escrow| {
        Some(match escrows {
            None => escrow.into_error(),
            Some(err) => err.context(format!(
                "another openPool or topUp of this command may still escrow USDC: {escrow}"
            )),
        })
    });
    let stopped = match (stopped, escrows) {
        (stopped, None) => stopped,
        (None, Some(escrows)) => Some(escrows),
        (Some(err), Some(escrows)) => Some(escrows.context(format!(
            "the pull stopped ({}), and an openPool or topUp of this command may have escrowed \
             USDC no record credits",
            sanitize_err_chain(&err)
        ))),
    };
    let stopped = stopped.map(|err| match (rerun, err.downcast_ref::<PoolReplaced>()) {
        (Some(_), Some(_)) => err.context(
            "funding needed: the pool this pull opened accepts no more funds either; run the \
             pull again",
        ),
        _ => err,
    });
    match stopped {
        None => BundleEnd::Complete,
        Some(error) => match error.downcast_ref::<FundingNeeded>().copied() {
            Some(needed) => BundleEnd::FundingNeeded { needed, error },
            None => BundleEnd::Stopped(error),
        },
    }
}

/// Whether `err` tells its caller to rerun the pull, or to fund the pool and
/// rerun it: a rerun escrows again while an untracked escrow is held, so such
/// an error never stands for one, even when its text names the escrow's tx.
fn invites_rerun(err: &anyhow::Error) -> bool {
    err.downcast_ref::<crate::GaveUp>().is_some()
        || err.downcast_ref::<PoolReplaced>().is_some()
        || err.downcast_ref::<FundingNeeded>().is_some()
}

/// One run's result from its `first` pass and its `second` pass: against a
/// pool a replacement opened, or on the next pool of the route. Both passes
/// walk the same
/// entries in the same order, so an entry the first pass landed and the
/// second found present keeps the first pass's outcome, and so does any
/// entry the second did not run; every other entry takes the second's. The two transfers add up.
fn merge_passes(first: PullRun, second: PullRun) -> PullRun {
    let outcomes = if first.outcomes.len() == second.outcomes.len() {
        first
            .outcomes
            .into_iter()
            .zip(second.outcomes)
            .map(|(was, now)| match (&was, &now) {
                (
                    EntryOutcome::Fetched(_) | EntryOutcome::Linked | EntryOutcome::Deduped(_),
                    EntryOutcome::Skipped,
                )
                | (_, EntryOutcome::NotRun) => was,
                _ => now,
            })
            .collect()
    } else {
        second.outcomes
    };
    let mut warnings = first.warnings;
    warnings.extend(second.warnings);
    PullRun {
        outcomes,
        warnings,
        transfer: first.transfer.add(second.transfer),
        interrupted: second.interrupted,
        stopped: second.stopped,
    }
}

/// Record one group's run in [`PullCtx::pull_groups`]: forward its recordable
/// files to the skip-cache flush, put its outcomes in slot `i`, and return the
/// fault that ends the whole pull, if its fetch failed with one.
fn settle_group_run(
    groups: &mut [SettledGroup],
    flush_tx: &tokio::sync::mpsc::UnboundedSender<FlushBatch>,
    i: usize,
    run: GroupRun,
    updates: BTreeMap<String, skip_cache::SavedFile>,
    warnings: Vec<String>,
) -> Option<anyhow::Error> {
    if !updates.is_empty() {
        // Newly-fetched content bytes drive the byte-cadence flush; a link/skip
        // records a file but lands no new bytes.
        let fetched_bytes = run
            .outcomes
            .iter()
            .map(|o| match o {
                EntryOutcome::Fetched(n) => *n,
                _ => 0,
            })
            .sum();
        // Unbounded send: fails only if the flush task is gone, which never
        // happens before `drive` drops `flush_tx`.
        let _ = flush_tx.send(FlushBatch {
            updates,
            fetched_bytes,
        });
    }
    if let Some(slot) = groups.get_mut(i) {
        *slot = SettledGroup {
            outcomes: run.outcomes,
            warnings,
            bytes: run.bytes,
        };
    }
    run.stop
}

/// A hash-group's result, kept for the run summary: its per-entry outcomes
/// and the content bytes its landed fetch paid for and resumed (see
/// [`GroupRun::bytes`]).
#[derive(Default)]
struct SettledGroup {
    outcomes: Vec<EntryOutcome>,
    /// The group's size warnings ([`size_warnings`]).
    warnings: Vec<String>,
    bytes: Option<EntryBytes>,
}

impl SettledGroup {
    /// The result of a group of `entries` entries the pull never settled:
    /// each entry not run.
    fn not_run(entries: usize) -> Self {
        Self {
            outcomes: std::iter::repeat_with(|| EntryOutcome::NotRun)
                .take(entries)
                .collect(),
            ..Self::default()
        }
    }
}

/// Run `items` through `run` once, at most `jobs` at a time. A bundle pull
/// passes every group: its [`JobGate`] is the jobs cap, and a group waits
/// for its slot inside `run`.
///
/// `settle` sees every result as it lands, with the item's index in `items`,
/// and returns the fault that ends the whole run, if the result carries one.
///
/// Once `settle` returns that fault, no further item starts, and this calls
/// `halt` once: every item still in flight stops at its next wait or loop
/// pass ([`crate::StopPolicy`]). An item can be in the middle of an on-chain
/// `topUp` or `openPool` lane build, so this waits for the items, for at most
/// [`GROUP_DRAIN`], and `settle` sees each one that lands. Each later fault
/// `settle` returns that may have escrowed USDC no record credits
/// ([`carries_untracked_escrow`]) is folded into the first fault the way one
/// fetch folds a late lane build's ([`fold_late_escrow`]): the escrow wins, so
/// a [`crate::GaveUp`] cannot send the caller into a rerun that escrows
/// again. Every other later fault is logged and dropped: the run already
/// ends. An item still running at the bound is dropped where it stands.
///
/// Returns the first fault, or the late escrow with the first fault folded
/// into it as text. `None` means every item ran and settled.
async fn run_groups<G, R, Fut>(
    items: Vec<G>,
    jobs: usize,
    run: impl Fn(G) -> Fut,
    mut settle: impl FnMut(usize, R) -> Option<anyhow::Error>,
    halt: impl FnOnce(),
) -> Option<anyhow::Error>
where
    Fut: std::future::Future<Output = R>,
{
    let width = jobs.min(items.len()).max(1);
    // Set once the run ends: no item starts after it.
    let ended = std::cell::Cell::new(false);
    // Items started and items settled, for the count a lapsed drain drops.
    let (started, settled) = (std::cell::Cell::new(0usize), std::cell::Cell::new(0usize));
    let mut stream = futures_util::stream::iter(items.into_iter().enumerate())
        .take_while(|_| std::future::ready(!ended.get()))
        .map(|(i, item)| {
            started.set(started.get() + 1);
            let fut = run(item);
            async move { (i, fut.await) }
        })
        .buffer_unordered(width);
    let stop = loop {
        let (i, result) = stream.next().await?;
        settled.set(settled.get() + 1);
        if let Some(err) = settle(i, result) {
            break err;
        }
    };
    ended.set(true);
    halt();
    let mut late: Option<anyhow::Error> = None;
    let drained = tokio::time::timeout(GROUP_DRAIN, async {
        while let Some((i, result)) = stream.next().await {
            settled.set(settled.get() + 1);
            let Some(err) = settle(i, result) else {
                continue;
            };
            if !carries_untracked_escrow(&err) {
                tracing::warn!(
                    item = i,
                    error = %sanitize_err_chain(&err),
                    "a fault that ends the pull, after another one ended it, is dropped"
                );
                continue;
            }
            late = Some(fold_escrow(
                late.take(),
                err,
                "another entry may also have escrowed USDC no record credits as the pull ended",
            ));
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            entries = started.get().saturating_sub(settled.get()),
            "bundle entries still running {GROUP_DRAIN:?} after the pull ended are dropped; \
             an openPool or topUp one of them sent is named as unconfirmed"
        );
    }
    fold_late_escrow(Err::<(), _>(stop), late).err()
}

/// The fault a pass of [`PullCtx::pull_groups`] ends with: `stopped`, the one
/// [`run_groups`] returned. A pass that `halted` with no fault ran on a halt
/// left over from before it, so its halted entries never ran: it fails
/// rather than end complete with nothing pulled.
fn pass_stop(stopped: Option<anyhow::Error>, halted: bool) -> Option<anyhow::Error> {
    match stopped {
        None if halted => Some(anyhow!(
            "the pull ran on a halt no entry of it set, so its halted entries never ran; run \
             the pull again"
        )),
        stopped => stopped,
    }
}

/// How long [`run_groups`] waits for the items still in flight once one ends
/// the run: one lane-build drain ([`BUILD_DRAIN`]) plus 10 s for an item to
/// reach it. A recovery step in progress when the halt lands runs first, and
/// can outlast the bound.
const GROUP_DRAIN: std::time::Duration =
    BUILD_DRAIN.saturating_add(std::time::Duration::from_secs(10));

/// The run's jobs cap ([`PullCtx::gate`]): one permit per hash group at work.
/// The semaphore is fair, so groups take their permits in start order.
pub(super) struct JobGate {
    permits: tokio::sync::Semaphore,
}

impl JobGate {
    /// A gate of `jobs` permits.
    pub(super) fn new(jobs: usize) -> Self {
        Self {
            permits: tokio::sync::Semaphore::new(jobs),
        }
    }
}

/// One jobs permit of a [`JobGate`], held by one hash group's fetch or by
/// the manifest fetch for as long as it runs.
struct FetchSlot<'g> {
    _permit: tokio::sync::SemaphorePermit<'g>,
}

impl<'g> FetchSlot<'g> {
    /// Wait for a permit of `gate`.
    async fn acquire(gate: &'g JobGate) -> anyhow::Result<Self> {
        let permit = gate
            .permits
            .acquire()
            .await
            .map_err(|_| anyhow!("bundle pull concurrency gate closed"))?;
        Ok(Self { _permit: permit })
    }
}

/// What a bundle run's lanes sign under.
pub(super) enum RunPayer {
    /// The buyer's own pool: the owner key owns it, the lane key signs.
    Owned(OwnedPayer),
    /// The delegated credential slot, read at each lane build so a swapped-in
    /// credential takes over at the next voucher boundary. A funding recovery
    /// step has no funding path: the delegate owns no pool to fund.
    Delegated(CredentialSlot),
}

/// One payment pool a bundle run pays through in a pass (ADR 005 § Payment
/// pool routing), and what the run's lanes on it share. A pool id is unique
/// only inside one `PaymentPool` contract, so lanes, funding facts and the
/// recovery gate never cross pools.
pub(super) struct PassPool<P: Provider + Clone> {
    /// The pool's contract and its payment chain's RPC endpoint.
    pub(super) pool: PaymentPoolChain,
    pub(super) contract: PaymentPool::PaymentPoolInstance<P>,
    pub(super) rpc: P,
    /// The pool's settlement token (USDC), read once via `PaymentPool.usdc()`.
    pub(super) token: Address,
    pub(super) voucher_dom: Eip712Domain,
    /// The per-lane voucher ledgers (ADR 039): every entry and chunk on a
    /// `(pool_id, signer, provider)` lane draws on one monotonic issuer, so
    /// concurrent same-lane fetches never race the cumulative watermark.
    pub(super) ledgers: LaneLedgers,
    /// The funding facts ([`RunFunding`]): the pool's spend outside the
    /// run's lanes, and a wallet too short of USDC to top it up.
    pub(super) funding: RunFunding,
    /// The funding recovery gate (ADR 003 § Funding recovery). A bundle pull
    /// is one fetch for every entry, so its entries share one progress rule
    /// on the pool, and so does the pass that runs the remaining entries
    /// again against a replaced pool.
    pub(super) recovery: Arc<RecoveryGate>,
}

impl<P: Provider + Clone> PassPool<P> {
    /// The pool `pool`, reached through `rpc`, whose token is `token`, with
    /// fresh lanes and recovery gate, reporting its escrows to `escrows`.
    pub(super) fn new(
        pool: PaymentPoolChain,
        rpc: P,
        token: Address,
        escrows: &UnconfirmedEscrows,
    ) -> Self {
        let deployment = pool.deployment;
        Self {
            contract: PaymentPool::new(deployment.payment_pool, rpc.clone()),
            rpc,
            token,
            voucher_dom: deployment.voucher_domain(),
            ledgers: LaneLedgers::new(),
            funding: RunFunding::reporting_escrows_to(escrows.for_pool(deployment)),
            recovery: Arc::new(RecoveryGate::new()),
            pool,
        }
    }
}

/// Builds the provider of a payment pool's chain ([`PullCtx::connect`]).
pub(super) type Connect<P> = Box<dyn Fn(&PaymentPoolChain) -> anyhow::Result<P> + Send + Sync>;

/// What one pass of a pull ([`PullCtx::pool_pass`]) ran: its entries, the
/// pool replacement its rerun ran against, and the bytes it paid for and
/// received on the wire.
struct PassRun {
    run: PullRun,
    rerun: Option<PoolReplaced>,
    paid: alloy::primitives::U256,
    wire: u64,
}

/// Whether a pass left work for a pass on another pool: an entry failed or
/// did not run, or a fault ended it. An interrupted pass leaves none: its
/// caller stopped the pull.
fn leaves_work(run: &PullRun) -> bool {
    !run.interrupted
        && (run.stopped.is_some()
            || run
                .outcomes
                .iter()
                .any(|o| matches!(o, EntryOutcome::Failed { .. } | EntryOutcome::NotRun)))
}

/// The run's state, shared by the entry fetch loop. Borrowed by every
/// in-flight entry future, and every entry must be polled on one task, as
/// `buffer_unordered` does: each entry reads its lanes' ledgers and queues
/// their watermark writes with no await between ([`crate::lanes`]), and only
/// one task keeps another entry's read from falling between them.
pub(super) struct PullCtx<P: Provider + Clone> {
    pub(super) endpoint: Endpoint,
    pub(super) store: Arc<dyn BuyerPoolStore>,
    /// The keys every entry's lanes sign under.
    pub(super) payer: RunPayer,
    pub(super) self_address: Address,
    pub(super) stream_dom: Eip712Domain,
    pub(super) chain: ChainConfig,
    /// The payment pool the current pass pays through, with its lanes,
    /// funding facts and recovery gate. The first pass pays through the first
    /// pool of the buyer's route
    /// ([`PoolRoute::for_buyer`](crate::pool_route::PoolRoute::for_buyer)).
    pub(super) pass: PassPool<P>,
    /// The pools of earlier passes, by contract, kept for their recovery
    /// gates and funding facts, and for a later pass on the same pool.
    pub(super) parked: HashMap<Deployment, PassPool<P>>,
    /// Builds the provider of a pool a later pass pays through.
    pub(super) connect: Connect<P>,
    /// The escrow handle every pool's funding facts report to
    /// ([`UnconfirmedEscrows::for_pool`]).
    pub(super) escrows: UnconfirmedEscrows,
    /// The run's routing of each node to a pool: a pass pays only the nodes
    /// routed to [`Self::pass`]'s pool. Each manifest fetch and each pull
    /// starts its passes afresh.
    pub(super) passes: PoolPasses,
    pub(super) site: PullSite,
    pub(super) relays: Vec<RelayUrl>,
    /// The node every entry is pinned to, or every active registered node for
    /// an unpinned run to sample.
    pub(super) nodes: NodeSelection,
    /// The run's discovery settings: rediscovery, proxy warming, the max
    /// sources, and a pinned node's dial address. An entry's first holders
    /// come from [`Self::nodes`].
    pub(super) discovery: DiscoveryOptions,
    /// The most a lane pays per MiB; `0` sets no ceiling.
    pub(super) max_rate_per_mb: u64,
    /// The largest blob one entry fetches.
    pub(super) max_blob_bytes: u64,
    /// How the run funds its pool.
    pub(super) deposit: DepositPolicy,
    /// Bundle-level namespace id (ADR 002) applied to every paid pull in the run —
    /// the namespace as a big-endian `uint256`; `NO_NAMESPACE` when the
    /// caller names none.
    pub(super) namespace_id: [u8; 32],
    /// Serializes every pool open-or-reuse across the whole bundle. A pass's
    /// every entry shares ONE `PaymentPool` deposit (ADR 003), so
    /// distinct providers cannot open concurrently: its on-chain state (deposit,
    /// standing USDC allowance) is one resource regardless of which provider an
    /// entry is bound for. Taken around the whole open-or-reuse call (which may
    /// also perform a low-water top-up) and released before streaming, so
    /// delivery itself still runs concurrently once each entry has its context.
    pub(super) open_lock: tokio::sync::Mutex<()>,
    /// Global jobs cap (min 1) on hash groups at work, one permit per group
    /// ([`FetchSlot`]), taken before the group's file bar and held for its
    /// whole-blob fetch.
    pub(super) gate: JobGate,
    /// Per-provider cap on concurrent same-lane streams (the max lane streams,
    /// default 4). Every lane an entry's fetch builds holds one of its
    /// provider's permits, taken without waiting ([`LaneSources`]). Bounds only
    /// per-provider concurrency; the jobs cap still bounds cross-lane parallelism.
    pub(super) lane_cap: LaneStreamCap,
    /// The command-wide holder health every entry's fetch records into, so a
    /// holder that faults one entry cools for every entry.
    pub(super) health: Arc<PeerHealth>,
    /// The command's one connection per node: every entry's lanes open their
    /// streams on it.
    pub(super) connections: Connections,
    /// The command's blocking state writes, in queue order across every
    /// entry ([`OrderedWrites`]).
    pub(super) writes: OrderedWrites,
    /// The command's stop policy. Its progress clock is shared by every entry
    /// and the manifest fetch, so the pull gives up only once no entry has
    /// landed a verified byte for the whole limit.
    pub(super) stop: StopPolicy,
    /// The run's undelivered planned download, in content bytes, which a
    /// delegated pull prices its running-low signal on ([`WorkShare`]).
    pub(super) demand: Arc<RunDemand>,
    /// Where the run reports its progress; `None` reports nothing.
    pub(super) observer: Option<Arc<dyn BundleObserver>>,
    /// What explains a failed fetch to the caller's user; `None` leaves every
    /// error as the fetch returned it.
    pub(super) hints: Option<Arc<dyn FaultHints>>,
    /// Where every lane keeps the signed messages a pull receipt holds;
    /// `None` keeps nothing.
    pub(super) evidence: Option<Arc<ReceiptEvidence>>,
    /// `(rtt_ms, rate_per_mb)` of each node the run's discovery rounds heard
    /// from, at its lowest RTT ([`super::BundlePuller::rate_hints`]).
    pub(super) rate_samples: std::sync::Mutex<HashMap<PublicKey, (f64, u64)>>,
    /// The one store every entry and the manifest fetch keep their verified
    /// bytes in ([`RunStore`]).
    pub(super) run_store: RunStore,
}

impl<P: Provider + Clone> PullCtx<P> {
    /// The run's store for `out_root` ([`RunStore::open`]). A store that
    /// cannot open logs at `debug` and reads as none: the fetch that follows
    /// surfaces the fault.
    async fn store_at(&self, out_root: &Path) -> Option<&BlobDir> {
        self.run_store.open(out_root).await.map_or_else(
            |e| {
                tracing::debug!("could not open the download store: {e:#}");
                None
            },
            Some,
        )
    }

    /// The shared lane deps, assembled from `PullCtx`'s borrowed chain
    /// plumbing plus this run's per-fetch budgets — the same shape a single fetch
    /// builds, for every fetch of an entry ([`Self::acquire_entry`]).
    fn drive_deps(&self, max_blob_bytes: u64) -> anyhow::Result<LaneDeps<'_, P>> {
        Ok(LaneDeps {
            marks: None,
            endpoint: &self.endpoint,
            store: &self.store,
            contract: &self.pass.contract,
            rpc: &self.pass.rpc,
            stream_dom: &self.stream_dom,
            self_address: self.self_address,
            token: self.pass.token,
            chain: &self.chain,
            pool: &self.pass.pool,
            passes: &self.passes,
            site: &self.site,
            deposit: self.deposit,
            // The bundle-level namespace (ADR 002): routes any cache-miss origin
            // pull to that namespace's authorized origins. Applies uniformly to the
            // manifest blob and every entry — all funnel through here.
            namespace_id: self.namespace_id,
            max_rate_per_mb: self.max_rate_per_mb,
            max_blob_bytes,
            // Same shape as `fetch` (#1134): a node that accepts the connection and
            // never answers is as dead as one that stops mid-stream, so the same
            // budget bounds both stages. The pull carries no overall wall-clock cap —
            // `drive` never consults one — so it completes for any blob size as long
            // as the upstream keeps feeding it bytes.
            deadlines: PullDeadlines::new(STALL_WINDOW, STALL_WINDOW, 0)?,
            connections: &self.connections,
            writes: &self.writes,
            funding: &self.pass.funding,
            evidence: self.evidence.as_deref(),
        })
    }

    /// Fetch one whole blob into `staging` through the acquire loop (ADR 039),
    /// across the entry's holders ([`Self::entry_targets`]). See
    /// [`Self::acquire_entry`] for the fetch itself and what it returns.
    ///
    /// The caller holds a jobs permit ([`FetchSlot`]) for the whole fetch. The
    /// fetch stops on `stop`. `max_blob_bytes` caps the blob's size claim (`0`
    /// for no cap). `store` is where the fetch keeps its bytes (see
    /// [`Self::acquire_entry`]).
    async fn fetch_to_staging(
        &self,
        hash: [u8; 32],
        staging: &Path,
        progress: Option<&ProgressCallback>,
        stop: &StopPolicy,
        (max_blob_bytes, store): (u64, Option<BlobDir>),
    ) -> anyhow::Result<()> {
        let targets = self.entry_targets(hash).await?;
        self.acquire_entry(
            &targets,
            (hash, staging, store),
            progress,
            stop,
            max_blob_bytes,
        )
        .await
    }

    /// The run's store for an entry staged at `staging` ([`RunStore::open`]).
    ///
    /// # Errors
    ///
    /// The store cannot open.
    async fn entry_store(&self, staging: &Path) -> anyhow::Result<BlobDir> {
        Ok(self.run_store.open(out_root_of(staging)?).await?.clone())
    }

    /// One entry's holders: the pinned node, opened for the blob's proven
    /// size, or a fresh round of discovery opens over the entry's own
    /// registry sample (proxy-warming non-holders first when they help, then
    /// holders by rank). The opens name the run's payer and ask for the whole
    /// blob, so the entry's first fetch adopts the winner's stream as its
    /// first leg ([`LaneSources::first_claim`]). An entry resolves them once
    /// and every fetch of the entry reuses them. A resolution that fails for
    /// any reason but the configuration ([`holders_or_none`]) starts the
    /// entry with no holder, and its acquire loop discovers them.
    async fn entry_targets(&self, hash: [u8; 32]) -> anyhow::Result<ResolvedTargets> {
        holders_or_none(self.resolve_entry_targets(hash).await)
    }

    /// The context the run's discovery opens send
    /// ([`crate::funding::discovery_ctx`]): the run's payer, on the pool a
    /// built lane of the run already pays from when there is one.
    async fn discovery_ctx(&self) -> anyhow::Result<crate::PoolContext> {
        let payer = match &self.payer {
            RunPayer::Delegated(slot) => LanePayer::Delegated(slot),
            RunPayer::Owned(payer) => LanePayer::Owned(payer),
        };
        crate::funding::discovery_ctx(
            self.store.as_ref(),
            &self.pass.contract,
            payer,
            (&self.chain.governance, self.pass.pool.deployment),
            &self.endpoint,
            &self.pass.voucher_dom,
            self.pass.funding.pool_id(),
        )
        .await
        .map_err(|err| {
            anyhow::anyhow!("could not name the pool the discovery opens pay from: {err:#}")
        })
    }

    /// [`Self::entry_targets`] before a failure is sorted into a configuration
    /// fault or an empty start.
    async fn resolve_entry_targets(&self, hash: [u8; 32]) -> anyhow::Result<ResolvedTargets> {
        let ctx = match (self.discovery_ctx().await, &self.nodes.selection) {
            (Ok(ctx), _) => ctx,
            // A pinned node is the entry's only holder whatever its open
            // says, so a pool the opens cannot name costs only the size
            // hint: the lane opens its own first leg.
            (Err(err), Selection::Pinned(pinned)) => {
                tracing::warn!(
                    error = %sanitize_err_chain(&err),
                    "the pinned node is not opened before its lane"
                );
                return Ok(pinned_targets(pinned.node_id, pinned.provider));
            }
            (Err(err), Selection::Registry(_)) => return Err(err),
        };
        let spec = OpenSpec {
            conns: &self.connections,
            ctx: &ctx,
            namespace_id: self.namespace_id,
            span: discovery_span(hash, FirstLeg::Whole, self.max_blob_bytes),
            max_rate_per_mb: self.max_rate_per_mb,
        };
        match &self.nodes.selection {
            Selection::Pinned(pinned) => {
                let open = OpenOpts {
                    marks: None,
                    round: OpenRound::Stream,
                    known_size: None,
                    spec,
                };
                Ok(open_pinned(
                    &self.chain,
                    self.relays.first().cloned(),
                    *pinned,
                    hash,
                    open,
                )
                .await)
            }
            Selection::Registry(registry) => {
                // With one pool, the entry's round streams: its fetch starts
                // at the first verified holder, and the holders that answer
                // later join it. With several pools, the round settles first,
                // so an entry whose holders all take another pool sees each
                // of them, and leaves the entry to that pool's pass
                // ([`Self::acquire_entry`]).
                let round = if self.passes.route().order().len() > 1 {
                    OpenRound::Settle
                } else {
                    OpenRound::Stream
                };
                let candidates = registry.entry_candidates(&self.site);
                let targets = open_and_order(
                    &candidates,
                    self.relays.first(),
                    hash,
                    spec,
                    self.discovery.proxy_warming,
                    &self.stream_dom,
                    round,
                )
                .await?;
                self.record_rates(&targets.samples);
                Ok(targets)
            }
        }
    }

    /// Whether a holder in `targets` is routed to a pool other than the
    /// current pass's ([`crate::pool_route::PoolRoute::choose`]): the entry
    /// then waits for that pool's pass rather than for a holder this pass
    /// can pay.
    fn routed_elsewhere(&self, targets: &ResolvedTargets) -> bool {
        let pass = self.pass.pool.deployment;
        targets.candidates.iter().any(|candidate| {
            let hint = targets
                .pool_hints
                .get(&candidate.node_id)
                .map(Vec::as_slice);
            self.passes
                .route()
                .choose(hint)
                .is_some_and(|chosen| chosen != pass)
        })
    }

    /// Keep each `(node_id, rtt_ms, rate_per_mb)` sample, at the lowest RTT
    /// a node answered at.
    fn record_rates(&self, samples: &[(PublicKey, f64, u64)]) {
        let mut kept = self
            .rate_samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        crate::quote::keep_nearest(&mut kept, samples.iter().copied());
    }

    /// The rates of the run's nodes, nearest first ([`rate_hints_of`]): the
    /// pinned node, or every registry node the region allowlist keeps.
    pub(super) fn rate_hints(&self) -> Vec<crate::quote::RateHint> {
        let nodes: Vec<(PublicKey, Address)> = match &self.nodes.selection {
            Selection::Pinned(pinned) => vec![(pinned.node_id, pinned.provider)],
            Selection::Registry(registry) => registry
                .allowed(&self.site, usize::MAX)
                .iter()
                .map(|c| (c.node_id, c.eth_address))
                .collect(),
        };
        let samples = self
            .rate_samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let store = crate::PeerStore::open(&self.site.data_dir);
        rate_hints_of(&nodes, &samples, &store, now_secs())
    }

    /// Fill blob `hash` into `store` through the acquire loop, across
    /// `targets`' holders. The blob is exported to `staging` once every byte is
    /// present. An entry passes the run's store ([`Self::entry_store`]): a
    /// lost export of the blob in it ([`StaleStoreEntry`]) fails the entry and
    /// marks the store for removal at the run's end. The manifest passes `None`
    /// and keeps a store of its own beside `staging`, which goes with the
    /// export, so a later manifest fetch never meets a lost export of it.
    ///
    /// The entry's size is a proven one: a discovery answer's, or else a
    /// whole-blob first open's header. A manifest size that differs from the
    /// blob never fails the entry.
    ///
    /// A holder that faults cools in the command-wide [`PeerHealth`] and
    /// returns inside the same loop, so a holder that fails one entry is
    /// rested for every entry. The fetch ends on done, a fault only the user
    /// can fix, a unanimous verdict of the holders, or `stop`: the pull's own
    /// policy for an entry, which a sibling entry halts, and an unhalted copy
    /// on the same clock for a manifest. Every lane draws vouchers from the
    /// run's shared
    /// `LaneLedgers` (ADR 039), serializes its pool open-or-reuse on
    /// `open_lock`, and holds one of its provider's stream permits
    /// ([`LaneStreamCap`]). Each lane's voucher watermark is persisted before
    /// the result returns, and the store keeps what landed for a resume.
    ///
    /// `max_blob_bytes` caps the blob's size claim (`0` for no cap): an entry
    /// runs under the run's [`Self::max_blob_bytes`], a manifest under
    /// [`manifest_cap_bytes`].
    async fn acquire_entry(
        &self,
        targets: &ResolvedTargets,
        (hash, staging, store): ([u8; 32], &Path, Option<BlobDir>),
        progress: Option<&ProgressCallback>,
        stop: &StopPolicy,
        max_blob_bytes: u64,
    ) -> anyhow::Result<()> {
        let deps = self.drive_deps(max_blob_bytes)?;
        let sources = LaneSources::new(
            &deps,
            &self.discovery,
            &self.relays,
            match &self.payer {
                RunPayer::Delegated(slot) => LanePayer::Delegated(slot),
                RunPayer::Owned(payer) => LanePayer::Owned(payer),
            },
            &self.pass.voucher_dom,
            Some(&self.open_lock),
            Some(&self.pass.ledgers),
            Some(&self.lane_cap),
            &self.pass.recovery,
        );
        let holders = sources.holders_from(targets);
        if holders.is_empty() && self.routed_elsewhere(targets) {
            return Err(anyhow!(
                "every holder of this entry takes payment through another pool; that pool's \
                 pass fetches it"
            ));
        }
        // A fetch dropped by a cancel, or still running [`GROUP_DRAIN`] after
        // a sibling's command-wide fault ([`run_groups`]), queues every lane's
        // vouchers for recording too.
        let on_drop = SettleOnDrop::new(|| sources.persist_watermarks_detached());
        let result = async {
            // The entry's size is a proven one: a discovery answer's, or a
            // whole-blob open's header. A manifest's size is declared, and it
            // plans nothing.
            let claim = sources
                .first_claim(hash, holders, FirstLeg::Whole, &self.health, stop)
                .await?;
            let (total_bytes, holders) = (claim.total_bytes, claim.holders);
            // The max sources cap the lanes the entry stripes across at
            // once; every other holder waits as a reserve.
            let mut downloader = Downloader::new(&sources, sources.funder())
                .holders(holders)
                .health(Arc::clone(&self.health))
                .recovery(Arc::clone(&self.pass.recovery))
                .max_lanes(self.discovery.max_sources)
                .max_blob_bytes(max_blob_bytes)
                .run_demand(Arc::clone(&self.demand));
            if let Some(slot) = sources.credentials() {
                downloader = downloader.credentials(slot);
            }
            if let Some(store) = store {
                downloader = downloader.store(store);
            }
            Box::pin(downloader.fetch_to_paths_shared(
                &[DownloadTarget {
                    hash,
                    total_bytes,
                    dest: staging,
                }],
                &self.pass.ledgers,
                progress,
                stop,
            ))
            .await
            .map(|_paths| ())
        }
        .await;
        on_drop.disarm();
        sources.persist_watermarks().await;
        if result
            .as_ref()
            .is_err_and(|err| err.downcast_ref::<StaleStoreEntry>().is_some())
        {
            self.run_store.note_lost_export();
        }
        result.map_err(|err| match &self.hints {
            Some(hints) => hints.annotate(err, &sources.fault_context()),
            None => err,
        })
    }

    /// The manifest blob `hash`, read from `out_root`'s manifest cache when
    /// `use_cache` allows and one is cached, else fetched through the run's
    /// acquire loop and cached. A cache file that cannot be read, or fails its
    /// hash check, is fail-open: the observer gets the warning and the run
    /// fetches. A failed cache write is a warning too. The fetch runs one
    /// pass per pool that a holder is routed to, as a single-blob fetch does
    /// ([`Self::fetch_manifest_bytes`]).
    pub(super) async fn fetch_manifest(
        &mut self,
        hash: [u8; 32],
        out_root: &Path,
        use_cache: bool,
    ) -> anyhow::Result<super::BundleManifest> {
        let cached = cached_manifest(out_root, hash, use_cache, |warning| self.warn(warning));
        let bytes = if let Some(cached) = cached {
            cached
        } else {
            let fetched = match self.fetch_manifest_bytes(hash, out_root).await {
                Ok(fetched) => fetched,
                Err(err) => {
                    let err = if retire_allowed(&err) {
                        self.retire_stale_pools()
                            .await
                            .into_iter()
                            .fold(err, anyhow::Error::context)
                    } else {
                        err
                    };
                    return Err(manifest_fetch_error(err));
                }
            };
            if let Some(warning) = manifest_cache::store(out_root, hash, &fetched) {
                self.warn(&warning);
            }
            if let Some(evidence) = &self.evidence {
                evidence.note_landed(hash, u64::try_from(fetched.len()).unwrap_or(u64::MAX));
            }
            fetched
        };
        super::BundleManifest::parse(&bytes)
    }

    /// Report `warning` to the observer.
    fn warn(&self, warning: &str) {
        if let Some(observer) = &self.observer {
            observer.warning(warning);
        }
    }

    /// Every pool replacement the run's funding recovery steps made, on every
    /// pool a pass paid through ([`RecoveryGate::replacements`]).
    pub(super) fn replacements(&self) -> Vec<PoolReplaced> {
        std::iter::once(&self.pass)
            .chain(self.parked.values())
            .flat_map(|pass| pass.recovery.replacements())
            .collect()
    }

    /// Read again each pass's stored pool that its first lane build found no
    /// record of, once a manifest fetch or a pull failed, and drop its buyer
    /// row if the chain still shows none. Only a pool none of whose lanes paid
    /// for a byte qualifies ([`RunFunding::retire_stale_pool`]).
    async fn retire_stale_pools(&self) -> Vec<StalePool> {
        let mut retired = Vec::new();
        for pass in std::iter::once(&self.pass).chain(self.parked.values()) {
            if let Some(stale) = pass
                .funding
                .retire_stale_pool(&pass.contract, self.store.as_ref())
                .await
            {
                retired.push(stale);
            }
        }
        retired
    }

    /// The warning for a wallet seen to hold too little USDC to top a pool
    /// up, one per pool that saw one.
    pub(super) fn funding_shortfalls(&self) -> Vec<String> {
        std::iter::once(&self.pass)
            .chain(self.parked.values())
            .filter_map(|pass| pass.funding.shortfall())
            .collect()
    }

    /// Start the passes of a manifest fetch or a pull afresh, on the current
    /// pool: no node has taken its one retry yet (ADR 005 § Payment pool
    /// routing).
    fn restart_passes(&mut self) {
        self.passes = PoolPasses::new(self.passes.route().clone());
        self.passes.start(self.pass.pool.deployment);
    }

    /// Move the run onto pool `next` for the next pass. A pool an earlier
    /// pass paid through comes back with its lanes and funding facts; a new
    /// one is connected and its token read. The pool it leaves is parked.
    ///
    /// # Errors
    ///
    /// `next` is not configured, its provider cannot be built, or its token
    /// read fails. The run stays on its current pool.
    async fn switch_pool(&mut self, next: Deployment) -> anyhow::Result<()> {
        if self.pass.pool.deployment == next {
            return Ok(());
        }
        let pass = if let Some(pass) = self.parked.remove(&next) {
            pass
        } else {
            let pool =
                self.chain.pool(next).cloned().ok_or_else(|| {
                    anyhow!("payment pool {} is not configured", next.pool_account())
                })?;
            let rpc = (self.connect)(&pool)?;
            let token = PaymentPool::new(next.payment_pool, rpc.clone())
                .usdc()
                .call()
                .await
                .map_err(|e| anyhow::Error::new(e).context("read PaymentPool.usdc()"))?;
            PassPool::new(pool, rpc, token, &self.escrows)
        };
        let left = std::mem::replace(&mut self.pass, pass);
        self.parked.insert(left.pool.deployment, left);
        self.passes.start(next);
        Ok(())
    }

    /// The manifest blob `hash`, fetched into memory: one pass per pool that
    /// a holder is routed to, as a single-blob fetch runs them. A fault ends
    /// the pass, and the next pool's pass tries the holders routed to it. A
    /// pool replacement ends the fetch: the pull that follows pays from the
    /// new pool.
    async fn fetch_manifest_bytes(
        &mut self,
        hash: [u8; 32],
        out_root: &Path,
    ) -> anyhow::Result<Vec<u8>> {
        self.restart_passes();
        loop {
            let err = match self.fetch_to_memory(hash, out_root).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => err,
            };
            if err.downcast_ref::<PoolReplaced>().is_some() {
                return Err(err);
            }
            let Some(next) = self.passes.next() else {
                return Err(err);
            };
            tracing::info!(
                failed_pool = %self.pass.pool.deployment.pool_account(),
                next_pool = %next.pool_account(),
                error = %sanitize_err_chain(&err),
                "the manifest fetch on this payment pool ended; it goes on with the nodes \
                 that take payment through the next pool"
            );
            if let Err(switch) = self.switch_pool(next).await {
                return Err(err.context(format!(
                    "the next payment pool cannot be used: {}",
                    sanitize_err_chain(&switch)
                )));
            }
        }
    }

    /// Pull every entry of `manifest` into `out_root`, and say how it ended.
    ///
    /// The pull runs one pass per pool that an entry's holder is routed to
    /// (ADR 005 § Payment pool routing). A pass that leaves work
    /// ([`leaves_work`]) hands it to the next pool's pass: a node routed to
    /// that pool, or one that refused a pool as `Declined` and named that
    /// one, is paid there. Every pass walks every entry: a file an earlier
    /// pass completed is skipped, `overwrite` or not, and a partial file
    /// resumes.
    ///
    /// Inside a pool's pass, a funding recovery step that replaced the pool
    /// ends it, and the remaining entries run once more against the new pool
    /// ([`Self::pool_pass`]). The outcome lists every replacement the pull
    /// made, on every pool.
    pub(super) async fn run(
        &mut self,
        manifest: &super::BundleManifest,
        out_root: &Path,
        overwrite: bool,
        cancel: &CancellationToken,
    ) -> BundleOutcome {
        let entries = manifest.entries.as_slice();
        // Each skip-cache record names the bundle its file came from.
        let bundle = manifest.hash();
        if let Some(observer) = &self.observer {
            observer.run_started(total_content_bytes(entries));
        }
        // The replacements this pull's steps make are the ones not listed
        // before it.
        let replaced_before = self.replacements();
        self.restart_passes();
        let mut pass = self
            .pool_pass(entries, bundle, out_root, overwrite, cancel)
            .await;
        while leaves_work(&pass.run) {
            let Some(next) = self.passes.next() else {
                break;
            };
            tracing::info!(
                failed_pool = %self.pass.pool.deployment.pool_account(),
                next_pool = %next.pool_account(),
                "the pass on this payment pool left entries; the pull goes on with the nodes \
                 that take payment through the next pool"
            );
            if let Err(err) = self.switch_pool(next).await {
                let err = err.context(format!(
                    "the pull cannot go on through payment pool {}",
                    next.pool_account()
                ));
                match &pass.run.stopped {
                    Some(_) => tracing::warn!(
                        error = %sanitize_err_chain(&err),
                        "the next payment pool cannot be used"
                    ),
                    None => pass.run.stopped = Some(err),
                }
                break;
            }
            // The first pass already wrote every file it landed and recorded
            // it in the skip-cache, so a later pass skips it even under
            // `overwrite`.
            let later = self
                .pool_pass(entries, bundle, out_root, false, cancel)
                .await;
            pass = PassRun {
                run: merge_passes(pass.run, later.run),
                rerun: later.rerun,
                paid: pass.paid.saturating_add(later.paid),
                wire: pass.wire.saturating_add(later.wire),
            };
        }
        let PassRun {
            run,
            rerun,
            paid,
            wire,
        } = pass;
        let failed = leaves_work(&run);
        let PullRun {
            outcomes,
            mut warnings,
            transfer,
            interrupted,
            mut stopped,
        } = run;
        warnings.extend(self.funding_shortfalls());
        // A failed pull re-reads a stored pool every node refuses `NotFound`
        // because the chain held no record of it, and drops its row if the
        // chain still shows none, so the next run opens or adopts a pool (ADR
        // 003 § Funding recovery).
        if retires_stale_pools(failed, paid, rerun) {
            let stale = self.retire_stale_pools().await;
            stopped = fold_stale_pools(stopped, stale, &mut warnings);
        }
        let mut report = bundle_report(&outcomes, transfer, warnings);
        report.paid_bytes = u64::try_from(paid).unwrap_or(u64::MAX);
        report.wire_bytes = wire;
        let replaced = self
            .replacements()
            .into_iter()
            .filter(|made| !replaced_before.contains(made))
            .collect();
        let escrows = &self.escrows;
        let end = pull_end(
            interrupted,
            stopped,
            rerun,
            escrows.pending(),
            escrows.untracked(),
        );
        BundleOutcome {
            report,
            end,
            replaced,
        }
    }

    /// One pass of a pull on the current pool: every entry of `entries`.
    ///
    /// A funding recovery step that replaced the pool ends the pass. The
    /// remaining entries run once more against the new pool: lanes key on the
    /// pool, so that rerun starts with fresh ledgers and funding facts, and
    /// each entry resumes the partial data it holds (ADR 003 § Funding
    /// recovery). A second replacement that ends the rerun ends the pull
    /// "funding needed". A replacement that did not end the pass runs nothing
    /// again ([`replaced_pool`]).
    async fn pool_pass(
        &mut self,
        entries: &[ManifestEntry],
        bundle: [u8; 32],
        out_root: &Path,
        overwrite: bool,
        cancel: &CancellationToken,
    ) -> PassRun {
        // Loaded once per pass: the pre-pass skip decisions consult it, and it
        // seeds the incremental skip-cache flush inside `pull_all` — a run's
        // completed files are folded onto this prior and rewritten as the pull
        // progresses, so an interrupted pull leaves the files it landed
        // recorded rather than losing the whole index. The skip-cache is
        // advisory, so a flush failure is logged (inside the flush) and never
        // fails the pull.
        let saved = skip_cache::load(out_root);
        let pass = &mut self.pass;
        leave_carried_replacement(&pass.recovery, &mut pass.ledgers, &mut pass.funding);
        // The lanes outlive one pull (a manifest fetch or an earlier pull of
        // this puller drew on them), so this pass's payment is what the
        // lanes' paid bytes grew by while it ran. Its wire bytes are what the
        // lanes' wire bytes grew by, measured the same way.
        let paid_before = pass.ledgers.paid_bytes();
        let wire_before = pass.ledgers.wire_bytes();
        // A halt lasts one pass ([`run_groups`]). Clear it as each pass starts
        // and ends, so it halts neither the next pass, nor a later manifest
        // fetch or pull of this puller, nor a pull after one its caller
        // dropped mid-pass.
        self.stop = self.stop.next_pass();
        let mut run = self
            .pull_all(entries, bundle, out_root, overwrite, saved, cancel)
            .await;
        self.stop = self.stop.next_pass();
        let mut paid = self.pass.ledgers.paid_bytes().saturating_sub(paid_before);
        let mut wire = self.pass.ledgers.wire_bytes().saturating_sub(wire_before);
        let rerun = replaced_pool(&run);
        if let Some(new) = rerun {
            if let Some(observer) = &self.observer {
                observer.pool_replaced(&new);
            }
            // Every lane in the fresh registry is built during this rerun: its
            // paid bytes count from its seed and its wire bytes from zero, so
            // the rerun's payment and wire bytes are the new registry's whole
            // totals.
            let pass = &mut self.pass;
            start_pass_on_new_pool(&pass.recovery, &mut pass.ledgers, &mut pass.funding);
            let saved = skip_cache::load(out_root);
            let second = self
                .pull_all(entries, bundle, out_root, overwrite, saved, cancel)
                .await;
            self.stop = self.stop.next_pass();
            run = merge_passes(run, second);
            paid = paid.saturating_add(self.pass.ledgers.paid_bytes());
            wire = wire.saturating_add(self.pass.ledgers.wire_bytes());
        }
        PassRun {
            run,
            rerun,
            paid,
            wire,
        }
    }

    /// Fetch one blob fully into memory — used only for the bundle manifest
    /// itself when the caller names it by hash rather than reading it locally.
    /// Manifests are small (unlike bundle entries, which stream straight to
    /// their destination and never buffer the whole blob), so streaming into a
    /// staging file under `out_root` and reading it back is cheap; it also
    /// means a manifest fetch runs the same acquire loop, under the same
    /// health table and progress clock, as every entry. The fetch takes a
    /// proven size (a discovery answer's, a pinned node's included, or a
    /// whole-blob open's when there is none).
    /// The fetch runs under [`manifest_cap_bytes`], so a blob past the
    /// largest manifest fails once the fetch holds the cap, and pays for at
    /// most the cap.
    /// The staging file is removed once read back — a manifest fetch has
    /// nothing further to resume once its bytes are safely in memory.
    async fn fetch_to_memory(&self, hash: [u8; 32], out_root: &Path) -> anyhow::Result<Vec<u8>> {
        let staging = staging_path(out_root, hash)?;
        // The manifest blob fetch is silent (no bar): `progress` is disabled here
        // anyway, and the per-file bars belong to the entries, not the manifest.
        let _slot = FetchSlot::acquire(&self.gate).await?;
        // A manifest belongs to no pass: a halt a pull left set, by a caller
        // that dropped it mid-drain, does not stop it.
        let stop = self.stop.next_pass();
        // Its own store, beside `staging`: the bytes leave the store at once,
        // so nothing in the run's store refers to a file this removes.
        let cap = manifest_cap_bytes(self.max_blob_bytes);
        self.fetch_to_staging(hash, &staging, None, &stop, (cap, None))
            .await?;
        let bytes =
            std::fs::read(&staging).with_context(|| format!("read {}", staging.display()))?;
        remove_staging_off_runtime(&staging).await;
        Ok(bytes)
    }

    /// Fetch every entry into `out_root`, one unit of work per *distinct* blob
    /// hash: entries sharing a hash (one file at two bundle paths) are fetched and
    /// reconstructed once, then materialized at each path (#1306) — never fetched,
    /// nor *paid for*, twice. Each unit of work runs via `buffer_unordered` (no
    /// `tokio::spawn`, by choice — nothing here is `!Send`), every one in flight
    /// at once and waiting in start order for its slot of `PullCtx.gate`, the
    /// one jobs cap; a group's file bar appears only once it holds a slot.
    /// Parallelism comes from concurrent in-flight network I/O, while the run's
    /// shared `LaneLedgers` keep same-lane voucher issuance monotonic across
    /// every entry.
    async fn pull_all(
        &self,
        entries: &[ManifestEntry],
        bundle: [u8; 32],
        out_root: &Path,
        overwrite: bool,
        saved: SavedManifest,
        cancel: &CancellationToken,
    ) -> PullRun {
        let refs: Vec<&ManifestEntry> = entries.iter().collect();
        // The output directory, held open for the pass: every entry lands
        // beneath its handle. One that cannot be opened is a local fault only
        // the user can fix, so it ends the pull before any group starts.
        let landing = match Landing::open(out_root) {
            Ok(landing) => Arc::new(landing),
            Err(e) => return PullRun::stopped_before_start(entries.len(), e),
        };
        // Pre-pass: classify every entry against the output tree and the saved
        // skip-cache before any fetch, so an updated file at an already-present
        // path is written rather than silently skipped. The re-hash is this
        // command's own work, so the progress clock holds while it runs, and
        // the time since the manifest landed (a caller's selection
        // included) ends when it does.
        let hold = self.stop.clock.hold();
        let mut disk = resolve_disk_state(entries, &saved, out_root, overwrite).await;
        // The run's outstanding work starts at what the groups that still
        // fetch plan to pay for.
        let store = self.store_at(out_root).await;
        disk.work = planned_work(entries, &landing, overwrite, &disk, store).await;
        let outstanding = disk
            .work
            .values()
            .fold(0u64, |sum, work| sum.saturating_add(work.download));
        self.demand.plan(outstanding);
        drop(hold);
        // `saved` moves on to seed the incremental skip-cache flush: each
        // completed group's records fold onto it and rewrite the cache as the
        // pull progresses.
        self.pull_groups(&refs, bundle, &landing, overwrite, &disk, saved, cancel)
            .await
    }

    /// Fetch every distinct blob once (grouped by hash) and materialize it at each
    /// destination path (#1306), routing each group through [`Self::fetch_group`].
    /// `PullCtx.gate` caps the groups at work at the jobs cap.
    #[allow(clippy::too_many_arguments)]
    async fn pull_groups<'e>(
        &self,
        entries: &[&'e ManifestEntry],
        bundle: [u8; 32],
        landing: &Arc<Landing>,
        overwrite: bool,
        disk: &DiskState,
        saved: SavedManifest,
        cancel: &CancellationToken,
    ) -> PullRun {
        let groups_by_hash = order_groups_smallest_first(group_by_hash(entries));
        let group_count = groups_by_hash.len().max(1);
        // Completed groups' skip-cache records flow to the flush task over this
        // channel. Unbounded so a `send` from the fetch-driving side never blocks
        // (the flush write must never stall a fetch); the messages are one small
        // batch per completed group, so the channel stays shallow.
        let (flush_tx, flush_rx) = tokio::sync::mpsc::unbounded_channel::<FlushBatch>();

        // The fetch-driving side: run every group through `fetch_group` once,
        // every one in flight and each waiting there for its jobs slot. As
        // each run completes, forward its recordable files (built here while
        // the group's entries are in scope) to the flush task and keep its
        // outcomes for the run summary. A fault that ends the whole pull
        // ([`ends_the_pull`]) halts every group still in flight: none that
        // waits for a slot starts a fetch (one that links from an on-disk
        // donor still does), and each running fetch stops and drains its lane
        // builds ([`run_groups`]).
        // A group the pull never settles (dropped at `GROUP_DRAIN`, by a
        // cancel, or never started) keeps its entries not run.
        let mut groups: Vec<SettledGroup> = groups_by_hash
            .iter()
            .map(|g| SettledGroup::not_run(g.entries.len()))
            .collect();
        let mut stopped: Option<anyhow::Error> = None;
        let drive = async {
            let run = |group: HashGroup<'e>| {
                // Cheap clone of the group's entry refs so `fetch_group` can consume
                // `group` while we still correlate outcomes to entries.
                let group_entries: Vec<&'e ManifestEntry> = group.entries.clone();
                async move {
                    let run = self.fetch_group(group, landing, overwrite, disk).await;
                    // `fetch_group` returns one outcome per entry, in order;
                    // `build_completed_updates` relies on that to correlate a path
                    // to its outcome by position. Assert the invariant so a future
                    // divergence is caught in debug builds rather than silently
                    // truncating the zip.
                    debug_assert_eq!(
                        group_entries.len(),
                        run.outcomes.len(),
                        "fetch_group must return one outcome per entry"
                    );
                    let updates = build_completed_updates(
                        &group_entries,
                        &run.outcomes,
                        bundle,
                        landing.root(),
                    );
                    let warnings = size_warnings(&group_entries, &updates);
                    (run, updates, warnings)
                }
            };
            stopped = run_groups(
                groups_by_hash,
                // Every group in flight at once: each waits for its slot of the
                // global gate, the one jobs cap, in start order.
                group_count,
                run,
                |i, (run, updates, warnings)| {
                    settle_group_run(&mut groups, &flush_tx, i, run, updates, warnings)
                },
                || self.stop.halt(),
            )
            .await;
            stopped = pass_stop(stopped.take(), self.stop.is_halted());
            // Closing the channel tells the flush task to do its final write.
            drop(flush_tx);
        };
        // The flush task: fold each batch onto the prior skip-cache and rewrite it
        // atomically at the cadence, plus a final write when the channel closes.
        // Running here (joined, not awaited inside `drive`) keeps the blocking
        // write off the fetch-driving path — `join!` keeps polling `drive` while
        // this side awaits its write.
        let flush = flush_task(landing.root(), saved, flush_rx);

        let interrupted = drive_until_interrupted(drive, flush, cancel).await;
        // Byte tally is per-group (a blob pulled once, materialized to N paths),
        // so sum it before flattening away the group boundaries.
        let transfer = groups
            .iter()
            .map(|g| group_transfer(&g.outcomes, g.bytes))
            .fold(Transfer::default(), Transfer::add);
        let warnings = groups
            .iter_mut()
            .flat_map(|g| std::mem::take(&mut g.warnings))
            .collect();
        let outcomes = groups.into_iter().flat_map(|g| g.outcomes).collect();
        PullRun {
            outcomes,
            warnings,
            transfer,
            interrupted,
            stopped,
        }
    }

    /// Run [`Self::pull_entry_untimed`] and, when the entry lands, report it
    /// with its size and the time the whole entry took
    /// ([`BundleObserver::entry_landed`], #2120, #2189). Returns the entry's
    /// [`EntryBytes`] (see [`Self::pull_entry_untimed`]), or an error carrying
    /// [`Halted`] once a sibling ends the pull: before this entry starts, or
    /// mid-fetch (see [`crate::acquire`]).
    async fn pull_entry(
        &self,
        hash: [u8; 32],
        total: Option<u64>,
        staging: &Path,
        file: Option<&FileBar>,
    ) -> anyhow::Result<EntryBytes> {
        // A sibling ended the pull while this entry waited for its slot.
        self.stop.ensure_running()?;
        let started = std::time::Instant::now();
        self.pull_entry_untimed(hash, total, staging, file)
            .await
            .inspect(|&bytes| self.report_landed(hash, total, staging, bytes, started.elapsed()))
    }

    /// Report a landed entry to the observer: its size in `staging`, the time
    /// the whole entry took, and the bytes it paid for and resumed.
    /// A staging file it cannot stat reports the manifest's size `total`, or 0
    /// without one, after a debug line naming the path and the error. The
    /// receipt evidence, when the run keeps one, records the entry with its
    /// size; an entry whose size neither source knows stays out of the receipt,
    /// with a warning, rather than going in as 0 against the size a node
    /// signed.
    fn report_landed(
        &self,
        hash: [u8; 32],
        total: Option<u64>,
        staging: &Path,
        bytes: EntryBytes,
        elapsed: std::time::Duration,
    ) {
        if self.observer.is_none() && self.evidence.is_none() {
            return;
        }
        let size = std::fs::metadata(staging).map_or_else(
            |e| {
                tracing::debug!(
                    "could not stat {} for its entry line: {e}",
                    staging.display()
                );
                total
            },
            |m| Some(m.len()),
        );
        if let Some(evidence) = &self.evidence {
            if let Some(size) = size {
                evidence.note_landed(hash, size);
            } else {
                tracing::warn!(
                    "{} landed but its size is unknown; the receipt leaves it out",
                    blake3::Hash::from_bytes(hash).to_hex()
                );
            }
        }
        let size = size.unwrap_or(0);
        let Some(observer) = &self.observer else {
            return;
        };
        observer.entry_landed(&EntryLanded {
            hash,
            size,
            paid: bytes.paid,
            resumed: bytes.resumed,
            elapsed,
        });
    }

    /// Fetch one entry's blob into `staging`, the finalized per-hash staging file
    /// [`Self::fetch_group`] then materializes to each destination, through
    /// [`Self::fetch_to_staging`].
    ///
    /// Before it drives anything, the entry reads the bytes the run's store
    /// already holds of it from an earlier, interrupted run ([`resumed_spans`]); those
    /// are resumed, not paid. The paid count is the whole blob less its resumed
    /// bytes, and 0 for an already-finalized staging blob. It is a content
    /// count: bao proof overhead is not in it.
    async fn pull_entry_untimed(
        &self,
        hash: [u8; 32],
        total: Option<u64>,
        staging: &Path,
        file: Option<&FileBar>,
    ) -> anyhow::Result<EntryBytes> {
        // The byte-delivery callback drives the file bar's download and the total
        // bar's download meter; the `file` handle also carries the discovering
        // phase the byte callback cannot express.
        let progress = file.map(FileBar::callback);
        // An already-finalized `<hex>` staging blob — a prior run exported it, or
        // this run did and crashed in the export-to-materialize window — is the
        // complete blob. Materialize straight from it: `fetch_group` copies
        // `staging` to each destination, so re-hash it once as a cheap guard and
        // return. On a mismatch (a corrupt leftover) drop it and fall through to
        // a normal fetch.
        if staging.try_exists().unwrap_or(false) {
            let staging_buf = staging.to_path_buf();
            let verified = tokio::task::spawn_blocking(move || {
                hash_file(&staging_buf).is_ok_and(|got| got == hash)
            })
            .await
            .map_err(|e| anyhow!("staging verify task: {e}"))?;
            if verified {
                if let (Some(cb), Some(total)) = (progress, total) {
                    cb(total, total);
                }
                return Ok(EntryBytes::default());
            }
            remove_staging_off_runtime(staging).await;
        }

        // What earlier runs already fetched of this blob: read now, before
        // any drive of this run adds to it.
        let store = self.store_at(out_root_of(staging)?).await;
        let resumed = resumed_spans(store, hash).await;

        // Until the first byte lands, the entry is discovering holders — surfaced so
        // a slow or stalling discovery (a blob no asked node answers for) does not look
        // like a frozen empty bar. The delivery callback switches the row to its
        // download counts on the first chunk.
        if let Some(f) = file {
            f.set_discovering();
        }

        // The finalized-staging fast path above already returned, so `staging`
        // does not exist here. The paid count is the verified blob's length,
        // less what an earlier run left.
        let store = self.entry_store(staging).await?;
        self.fetch_to_staging(
            hash,
            staging,
            progress,
            &self.stop,
            (self.max_blob_bytes, Some(store)),
        )
        .await?;
        let len = tokio::fs::metadata(staging).await.map_or(0, |m| m.len());
        Ok(EntryBytes::whole_blob(len, span_bytes(&resumed)))
    }

    /// The group's share of the run's outstanding work: the work the
    /// pre-pass planned for it ([`DiskState::work`]), which stays outstanding
    /// until it lands or the group settles, whichever way it settles. A group
    /// the pre-pass plans no work for holds an empty share.
    fn work_share(&self, group: &HashGroup<'_>, disk: &DiskState) -> Arc<WorkShare> {
        let work = disk.work.get(&group.hash).copied().unwrap_or_default();
        Arc::new(WorkShare::new(Arc::clone(&self.demand), work))
    }

    /// Fetch the blob shared by one hash-group and write it under `out_root` at
    /// each entry's path. The blob is fetched — and paid for — **once**; the first
    /// writable destination receives the materialized bytes and every other is a
    /// hard link (or copy) of it (#1306). Returns one [`EntryOutcome`] per input
    /// entry, in order. Never panics or short-circuits — every failure becomes an
    /// [`EntryOutcome::Failed`], and the [`GroupRun`] carries a fetch fault that
    /// ends the whole pull ([`ends_the_pull`]).
    async fn fetch_group(
        &self,
        group: HashGroup<'_>,
        landing: &Arc<Landing>,
        overwrite: bool,
        disk: &DiskState,
    ) -> GroupRun {
        let share = self.work_share(&group, disk);
        let hash = group.hash;
        // Wait for the group's jobs slot: every group of the run is in flight at
        // once, and this is the cap.
        let _slot = match FetchSlot::acquire(&self.gate).await {
            Ok(slot) => slot,
            Err(e) => {
                return GroupRun::done(
                    group
                        .entries
                        .iter()
                        .map(|en| EntryOutcome::failed(&en.path, &e))
                        .collect(),
                );
            }
        };

        let total = group.size();
        let slots = plan_slots(&group.entries, landing, overwrite, disk);

        // Every destination already present (or failed to resolve) → no fetch, no
        // payment. This is the whole point of the group: a duplicate path that is
        // already on disk costs nothing. It downloads nothing, so it contributes
        // nothing to the total bar's download meter — no credit, no bar.
        if !slots.iter().any(|s| matches!(s, Slot::Write { .. })) {
            return GroupRun::done(
                slots
                    .into_iter()
                    .map(|s| match s {
                        Slot::Failed(o) => o,
                        _ => EntryOutcome::Skipped,
                    })
                    .collect(),
            );
        }

        // Whole-file dedup: an identical file already sits somewhere in the root
        // (recorded by a prior bundle). Verify that candidate by re-hashing it,
        // then materialize every destination by link/copy — no fetch, no payment.
        // A failed re-hash (stale/edited/removed donor) falls through to the
        // normal fetch path below; `slots` is untouched until this decision is
        // made, so falling through loses nothing. `materialize_group` (inside
        // `materialize_from_donor`) already passes a `Slot::Failed` through as its
        // own failure and a `Slot::Skip` through as `Skipped`, so handing it the
        // whole `slots` vector — not just the `Write` entries — is correct.
        if let Some(donor) = disk.whole_file.get(&hash)
            && let Some(len) = verified_donor_len(donor, hash).await
        {
            // A whole-file link downloads nothing, so it adds nothing to the
            // total bar's download meter.
            return GroupRun::done(materialize_from_donor(landing, slots, donor, len).await);
        }

        // Staged once per group: the entry's fetch finalizes to
        // `out_root/.decdn-partial/<hex>` (#1497), streaming into the run's store
        // rather than buffering the blob, which is what lets a large entry top
        // up mid-fetch. The staging path derived from `out_root`
        // is only created once a group actually has something to write, never for
        // an all-skipped group.
        let staging = match staging_path(landing.root(), hash) {
            Ok(p) => p,
            Err(e) => return GroupRun::done(fail_all(slots, &e)),
        };

        // One per-file bar for this group's single pull, labeled by its destination
        // path(s) — a fetch-once hash group shows one bar for every path it lands at.
        let paths: Vec<String> = group.entries.iter().map(|e| e.path.clone()).collect();
        let file_bar = FileBar::of(
            self.observer
                .as_ref()
                .map(|observer| observer.file_started(&paths, total.unwrap_or(0))),
            share,
        );
        let fetched = self
            .pull_entry(hash, total, &staging, Some(&file_bar))
            .await;
        file_bar.finish();
        let bytes = match fetched {
            Ok(bytes) => bytes,
            Err(e) => {
                // `pull_entry` leaves what landed in the run's store on
                // error: it is what the next run resumes from rather than
                // re-paying for bytes already landed.
                return GroupRun::fetch_failed(slots, e);
            }
        };

        // A failed first write leaves `staging` in place, so the next writable
        // path retries from it. Off the runtime: a multi-GB copy on this task
        // would stall every other group's streams.
        let outcomes = materialize_group(
            slots,
            |dest| {
                let (staging, landing) = (staging.clone(), landing.clone());
                off_runtime("materialize", move || {
                    landing.move_into_place(&staging, &dest)
                })
            },
            |src, dest| link_off_runtime(landing.clone(), src, dest),
        )
        .await;

        // Remove the paid staging blob (and its sidecars) ONLY when every
        // destination landed. If any materialize failed (unwritable dir, ENOSPC, a
        // link failure), keep it: the blob is fully fetched and paid for, and
        // `pull_entry` already finalized `<hex>` — a rerun sees the finalized
        // staging file and re-pulls only the still-missing ranges (typically
        // none), never the whole blob. Deleting staging here would force a full
        // re-fetch — and re-payment — of an unrefunded blob in *every* case;
        // `fetch`'s single-blob path gets this free from its own ranged store, so
        // the copy-based fan-out must gate it. A blob that moved into its first
        // destination leaves no staging blob behind; that destination is recorded
        // for the skip cache, so a rerun reuses it as a whole-file donor.
        let any_failed = outcomes
            .iter()
            .any(|o| matches!(o, EntryOutcome::Failed { .. }));
        if !any_failed {
            remove_staging_off_runtime(&staging).await;
        }

        GroupRun::landed(outcomes, bytes)
    }
}

/// One hash group's share of the run's outstanding work: its planned
/// download bytes, taken off the run's total as they land, and whatever is
/// left once the group settles, however it settles. A delegated pull prices
/// its running-low signal on that total ([`crate::Downloader::run_demand`]).
struct WorkShare {
    /// The run's demand, whose planned work this share is part of.
    demand: Arc<RunDemand>,
    /// The group's planned download bytes.
    planned: u64,
    /// The prefix an earlier run left, which a fetch's position counts first.
    resumed: u64,
    /// The planned bytes already taken off the run's planned work.
    taken: AtomicU64,
}

impl WorkShare {
    /// A share of `work` in `demand`.
    const fn new(demand: Arc<RunDemand>, work: PlannedWork) -> Self {
        Self {
            demand,
            planned: work.download,
            resumed: work.resumed,
            taken: AtomicU64::new(0),
        }
    }

    /// The group's fetch has reached position `received`, its resumed prefix
    /// included: take what is new this run, up to the group's plan, off the
    /// run's total.
    fn delivered(&self, received: u64) {
        let capped = received.saturating_sub(self.resumed).min(self.planned);
        let before = self.taken.fetch_max(capped, Ordering::Relaxed);
        if capped > before {
            let gone = capped - before;
            self.demand.done(gone);
        }
    }
}

impl Drop for WorkShare {
    /// A settled group plans no more work.
    fn drop(&mut self) {
        self.delivered(self.resumed.saturating_add(self.planned));
    }
}

/// One hash group's progress handle: the caller's [`FileObserver`], the
/// group's [`WorkShare`], and the delivery callback that feeds both.
pub(super) struct FileBar {
    file: Option<Arc<dyn FileObserver>>,
    cb: Box<ProgressCallback>,
}

impl FileBar {
    /// The handle over `file` and `share`.
    fn of(file: Option<Arc<dyn FileObserver>>, share: Arc<WorkShare>) -> Self {
        let observer = file.clone();
        let cb = Box::new(move |received: u64, expected: u64| {
            share.delivered(received);
            if let Some(file) = &observer {
                file.delivered(received, expected);
            }
        }) as Box<ProgressCallback>;
        Self { file, cb }
    }

    /// The delivery callback to hand the fetch path.
    fn callback(&self) -> &ProgressCallback {
        self.cb.as_ref()
    }

    /// Report that the entry discovers its holders before any byte arrives.
    fn set_discovering(&self) {
        if let Some(file) = &self.file {
            file.discovering();
        }
    }

    /// Report that the group's pull settled, landed or failed.
    fn finish(self) {
        if let Some(file) = self.file {
            file.finished();
        }
    }
}

/// The content bytes one landed entry paid for and resumed from an earlier
/// run, as [`PullCtx::pull_entry_untimed`] counts them. No byte counts in
/// both.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EntryBytes {
    /// Content bytes this run fetched and paid for (see
    /// [`PullCtx::pull_entry_untimed`]).
    paid: u64,
    /// Bytes an earlier, interrupted run fetched into the entry's store,
    /// which this run resumed rather than fetched again.
    resumed: u64,
}

impl EntryBytes {
    /// A whole-blob fetch that landed a `len`-byte blob into a store that
    /// already held `resumed` bytes of it.
    fn whole_blob(len: u64, resumed: u64) -> Self {
        let resumed = resumed.min(len);
        Self {
            paid: len.saturating_sub(resumed),
            resumed,
        }
    }
}

/// The byte spans of blob `hash` that earlier runs already fetched into
/// `store`, as sorted, disjoint `(offset, len)` pairs: bytes an earlier run
/// paid for. No store gives no spans.
///
/// The spans are for reporting only: a store it cannot read logs at `debug`
/// and gives no spans, and the fetch that follows surfaces the fault.
async fn resumed_spans(store: Option<&BlobDir>, hash: [u8; 32]) -> Vec<(u64, u64)> {
    let Some(store) = store else {
        return Vec::new();
    };
    // No bound clamps a view that only lists what the store holds.
    store.blob(hash, u64::MAX, None).await.map_or_else(
        |e| {
            tracing::debug!("could not read the download store: {e:#}");
            Vec::new()
        },
        |blob| blob.present_byte_ranges(),
    )
}

/// The total length of `(offset, len)` spans, saturating. The caller passes
/// disjoint spans, so no byte counts twice.
fn span_bytes(spans: &[(u64, u64)]) -> u64 {
    spans
        .iter()
        .fold(0u64, |acc, &(_, len)| acc.saturating_add(len))
}

/// BLAKE3 of the whole file at `path`, streamed so an arbitrarily large blob
/// never loads into memory at once.
fn hash_file(path: &Path) -> anyhow::Result<[u8; 32]> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f
            .read(&mut buf)
            .with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }
        let slice = buf
            .get(..n)
            .ok_or_else(|| anyhow!("short read buffer slice"))?;
        hasher.update(slice);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// The directory a run pulls into, given one of its staging paths
/// (`<out_root>/.decdn-partial/<hex>`, [`staging_path`]).
///
/// # Errors
///
/// `staging` is not two levels below a directory.
fn out_root_of(staging: &Path) -> anyhow::Result<&Path> {
    staging
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("staging path {} has no output root", staging.display()))
}

/// The blob cap a manifest fetch runs under, given the run's `max_blob_bytes`:
/// the lower of the two, with [`MAX_MANIFEST_MB`] for a run with no cap (`0`).
fn manifest_cap_bytes(max_blob_bytes: u64) -> u64 {
    let cap = MAX_MANIFEST_MB << 20;
    match max_blob_bytes {
        0 => cap,
        run => run.min(cap),
    }
}

/// Per-hash staging file an entry's fetch finalizes to before `fetch_group`
/// materializes it at the manifest's destination path(s) —
/// `<out_root>/.decdn-partial/<hex>` (#1497). This is the *plain* final path:
/// the run's store ([`RunStore`]) exports the blob to `<hex>` once complete.
/// The store is what survives an interrupted pull — a rerun resumes from it
/// rather than re-paying for bytes already landed.
/// Creates the staging directory (idempotent, and only ever called once a
/// group has real work to do — an all-skipped group never creates one) but
/// not the file itself; the store's export does that.
fn staging_path(out_root: &Path, hash: [u8; 32]) -> anyhow::Result<PathBuf> {
    let dir = out_root.join(STAGING_DIR);
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let name = blake3::Hash::from_bytes(hash).to_hex().to_string();
    Ok(dir.join(name))
}

/// [`remove_staging`] on the blocking pool: unlinking a multi-GB blob can take
/// long enough on some filesystems to stall the task every group shares. A
/// failed join only leaves harmless clutter, the same as a failed removal.
async fn remove_staging_off_runtime(staging: &Path) {
    let staging = staging.to_path_buf();
    if let Err(e) = tokio::task::spawn_blocking(move || remove_staging(&staging)).await {
        tracing::warn!("staging removal task: {e}");
    }
}

/// Best-effort cleanup of a finalized staging blob whose content is safely
/// elsewhere (materialized to disk, or read into memory) and so has nothing left
/// to resume. A leftover here is harmless clutter, not a correctness issue, so
/// a removal failure is reported and swallowed rather than propagated.
fn remove_staging(staging: &Path) {
    if let Err(e) = std::fs::remove_file(staging)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("failed to remove staging file {}: {e}", staging.display());
    }
}

/// Fail every `Write` slot with `e`, preserving `Failed`/`Skip` classification —
/// the shared tail of `fetch_group`'s two pre-materialize failure paths (a
/// staging-dir create error, or the fetch itself failing).
fn fail_all(slots: Vec<Slot<'_>>, e: &anyhow::Error) -> Vec<EntryOutcome> {
    slots
        .into_iter()
        .map(|s| match s {
            Slot::Failed(o) => o,
            Slot::Skip => EntryOutcome::Skipped,
            Slot::Write { label, .. } => EntryOutcome::failed(label, e),
        })
        .collect()
}

/// Mark every `Write` slot [`EntryOutcome::NotRun`], preserving
/// `Failed`/`Skip` classification, for a fetch a sibling halted.
fn not_run_all(slots: Vec<Slot<'_>>) -> Vec<EntryOutcome> {
    slots
        .into_iter()
        .map(|s| match s {
            Slot::Failed(o) => o,
            Slot::Skip => EntryOutcome::Skipped,
            Slot::Write { .. } => EntryOutcome::NotRun,
        })
        .collect()
}

impl EntryOutcome {
    fn failed(path: &str, err: &anyhow::Error) -> Self {
        Self::Failed {
            path: path.to_string(),
            // Sanitize here, at the single construction site: a per-entry fetch
            // failure can wrap a chain-RPC error whose source carries the
            // `rpc_url` (API key in path/query). The caller prints this outcome
            // and may serialize it into a report, and neither passes through a
            // process-level sanitizer (issue #954).
            err: sanitize_err_chain(err),
        }
    }
}

/// Resolve a bundle entry's POSIX relative `path` to a filesystem path under
/// `root`, rejecting anything that isn't a sequence of plain, portable filename
/// components — absolute paths, `.`/`..`, drive prefixes, empty components
/// (leading/trailing/double slash), and every segment
/// [`super::path_segment_problem`] refuses. With `..` rejected, the result
/// cannot escape `root`. (`appendix-bundles.md` § Path-safety rules.)
pub(super) fn safe_join(root: &Path, rel: &str) -> anyhow::Result<PathBuf> {
    if rel.is_empty() {
        bail!("empty entry path");
    }
    let mut out = root.to_path_buf();
    for seg in rel.split('/') {
        if let Some(why) = super::path_segment_problem(seg) {
            bail!("rejected unsafe component {seg:?} in entry path {rel:?}: {why}");
        }
        // Each `/`-split segment must be exactly one `Normal` path component.
        // This rejects "", ".", "..", "/", and a Windows drive prefix uniformly
        // and cross-platform.
        let mut comps = Path::new(seg).components();
        match (comps.next(), comps.next()) {
            (Some(Component::Normal(s)), None) => out.push(s),
            _ => bail!("rejected unsafe component {seg:?} in entry path {rel:?}"),
        }
    }
    Ok(out)
}

/// The skip-cache record for one entry of the bundle whose manifest hash is
/// `bundle`, built from its FINAL on-disk state, or `None` when this run has
/// nothing safe to record for it. A record is produced only when a regular file
/// is present at the entry's path with a usable mtime — the gates that keep a
/// symlink, a non-regular file, a missing file, or an unreadable mtime out of
/// the later fast-skip path. [`Landing`] renames new content into place only
/// on success, so a present regular file at the path is this run's landed
/// bytes; pairing the entry's hash with them is sound.
fn record_one(
    out_root: &Path,
    bundle: [u8; 32],
    en: &ManifestEntry,
) -> Option<skip_cache::SavedFile> {
    let dest = safe_join(out_root, &en.path).ok()?;
    // `symlink_metadata` does not follow links: only a regular file this run
    // landed is recorded, so a symlink or non-regular file at `dest` is never
    // written into the skip-cache (its later fast-skip would trust a target
    // this run never verified).
    let meta = std::fs::symlink_metadata(&dest).ok()?;
    if !meta.is_file() {
        return None; // only record regular files
    }
    let mtime = SavedMtime::of(&meta)?; // no usable mtime → omit the unverifiable gate
    Some(skip_cache::SavedFile {
        hash: format_hash(en.hash),
        size: meta.len(),
        mtime,
        source: skip_cache::SavedSource::new(bundle, en.manifest_path()),
    })
}

/// One line for each entry whose landed file differs in size from its
/// manifest's `size`: `<path>: manifest says X bytes, the blob is Y bytes`. A
/// manifest size is only declared, and the holders prove the true one, so
/// such an entry succeeds;
/// the line tells the user the manifest is stale. `updates` holds the landed
/// files' records ([`build_completed_updates`]).
fn size_warnings(
    entries: &[&ManifestEntry],
    updates: &BTreeMap<String, skip_cache::SavedFile>,
) -> Vec<String> {
    entries
        .iter()
        .filter_map(|en| {
            let manifest = en.size?;
            let landed = updates.get(&en.path)?.size;
            (landed != manifest).then(|| {
                format!(
                    "{}: manifest says {manifest} bytes, the blob is {landed} bytes",
                    en.path
                )
            })
        })
        .collect()
}

/// Build the skip-cache updates for a batch of `entries` paired with THIS run's
/// `outcomes` for them, in order — `fetch_group` returns one outcome per entry,
/// in order, so position correlates a path to its outcome. Only an entry whose
/// outcome is a success this run (`Fetched`, `Linked`, or `Skipped`) is
/// eligible; a `Failed` entry is omitted, because its fetch left the OLD bytes in
/// place (materialize renames new content only on success) and pairing the *new*
/// manifest hash with them would let a later mtime/hash fast path wrongly treat
/// the stale file as up to date and never re-fetch it.
///
/// Correlating by position — rather than "every entry not in the failed set" —
/// is what makes this safe to call MID-RUN: an entry whose group has not
/// completed yet is simply absent from `outcomes`, so it is never recorded from
/// bytes this run has not landed. A batch may be one completed group or the whole
/// run's outcomes; the result is the same records either way. Each record names
/// its entry's manifest path in the bundle whose manifest hash is `bundle` as
/// its source.
fn build_completed_updates(
    entries: &[&ManifestEntry],
    outcomes: &[EntryOutcome],
    bundle: [u8; 32],
    out_root: &Path,
) -> BTreeMap<String, skip_cache::SavedFile> {
    let mut updates = BTreeMap::new();
    for (en, outcome) in entries.iter().zip(outcomes) {
        let recordable = matches!(
            outcome,
            EntryOutcome::Fetched(_) | EntryOutcome::Linked | EntryOutcome::Skipped
        );
        if recordable && let Some(rec) = record_one(out_root, bundle, en) {
            updates.insert(en.path.clone(), rec);
        }
    }
    updates
}

/// Completed files may accumulate up to this many before the skip-cache is
/// rewritten, bounding how much a later run re-hashes to recover after an
/// interruption (and how often a many-file pull rewrites the cache).
const FLUSH_FILES: usize = 64;
/// Newly-fetched bytes may land up to this many before the skip-cache is
/// rewritten, so a long single-file transfer still checkpoints its completed
/// siblings without waiting for [`FLUSH_FILES`].
const FLUSH_BYTES: u64 = 1 << 30; // 1 GiB

/// One completed group's contribution to the skip-cache flush: the records to
/// fold in, and the new content bytes it fetched (which drive the byte cadence —
/// a link or skip records a file but lands no new bytes).
struct FlushBatch {
    updates: BTreeMap<String, skip_cache::SavedFile>,
    fetched_bytes: u64,
}

/// Fold each completed-group [`FlushBatch`] onto `acc` and rewrite the skip-cache
/// atomically whenever ≥[`FLUSH_FILES`] files or ≥[`FLUSH_BYTES`] new bytes have
/// accumulated since the last write, plus one final write when the channel closes
/// so the last partial batch is always persisted. This is the sole writer of the
/// skip-cache for the run: a single consumer, so its rewrites never race.
///
/// The skip-cache is advisory — a flush failure is logged and never propagated.
/// The blocking filesystem write runs on a blocking task with owned bytes, so the
/// caller's `join!` keeps driving fetches while a flush is in flight.
async fn flush_task(
    out_root: &Path,
    mut acc: SavedManifest,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<FlushBatch>,
) {
    let mut files_since = 0usize;
    let mut bytes_since = 0u64;
    let mut dirty = false;
    while let Some(batch) = rx.recv().await {
        files_since += batch.updates.len();
        bytes_since = bytes_since.saturating_add(batch.fetched_bytes);
        skip_cache::merge(&mut acc, batch.updates);
        dirty = true;
        if (files_since >= FLUSH_FILES || bytes_since >= FLUSH_BYTES)
            && flush_now(out_root, &acc).await
        {
            // Only clear on a successful write. A failed cadence flush keeps
            // `dirty` set and the counters over threshold, so the next batch
            // retries and the final write below still runs at close — the last
            // accumulated updates are never dropped by a transient write error.
            files_since = 0;
            bytes_since = 0;
            dirty = false;
        }
    }
    // Persist whatever landed since the last successful flush (or the only batch of
    // a small run). `dirty` stays false only when the last cadence flush already
    // wrote everything, so a clean run adds no redundant final write.
    if dirty {
        flush_now(out_root, &acc).await;
    }
}

/// Serialize `acc` (fast, in-memory) and write it atomically on a blocking task.
/// Returns whether the write succeeded so the caller can retry a failed cadence
/// flush at close. Advisory: every failure is logged, never returned as an error.
async fn flush_now(out_root: &Path, acc: &SavedManifest) -> bool {
    let bytes = match skip_cache::serialize(acc) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                "failed to serialize {}: {e}",
                skip_cache::SAVED_MANIFEST_NAME
            );
            return false;
        }
    };
    let root = out_root.to_path_buf();
    match tokio::task::spawn_blocking(move || skip_cache::write_bytes(&root, &bytes)).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::warn!("failed to write {}: {e}", skip_cache::SAVED_MANIFEST_NAME);
            false
        }
        Err(e) => {
            tracing::warn!("skip-cache flush task panicked: {e}");
            false
        }
    }
}

/// The manifest blob `hash` a pull reads from `out_root`'s manifest cache
/// instead of paying for a fetch, or `None` to fetch it. "Don't pay again": a
/// prior pull of this bundle into the same output dir cached the manifest
/// blob, keyed by its hash and self-verifying on read. Without `use_cache` (an
/// overwriting pull) it returns `None` to force a fresh fetch. A cache file
/// that cannot be read or fails its hash check is fail-open: `warn` gets the
/// line and the pull fetches.
fn cached_manifest(
    out_root: &Path,
    hash: [u8; 32],
    use_cache: bool,
    warn: impl FnOnce(&str),
) -> Option<Vec<u8>> {
    if !use_cache {
        return None;
    }
    manifest_cache::load(out_root, hash).unwrap_or_else(|e| {
        warn(&format!("{e:#}; fetching the bundle manifest"));
        None
    })
}

/// The run's [`BundleReport`]: the entry counts tallied from `outcomes`, beside
/// the byte tallies and the run's `warnings`.
fn bundle_report(
    outcomes: &[EntryOutcome],
    transfer: Transfer,
    warnings: Vec<String>,
) -> BundleReport {
    let mut report = BundleReport {
        downloaded: transfer.downloaded,
        reconstructed: transfer.reconstructed,
        resumed_bytes: transfer.resumed,
        warnings,
        ..BundleReport::default()
    };
    for o in outcomes {
        match o {
            EntryOutcome::Fetched(_) => report.fetched += 1,
            EntryOutcome::Linked => report.linked += 1,
            EntryOutcome::Skipped => report.skipped += 1,
            EntryOutcome::Deduped(n) => {
                report.reused += 1;
                report.reused_bytes = report.reused_bytes.saturating_add(*n);
            }
            EntryOutcome::Failed { path, err } => {
                report.failed += 1;
                report.failures.push(EntryFailure {
                    path: path.clone(),
                    error: err.clone(),
                });
            }
            EntryOutcome::NotRun => report.not_run += 1,
        }
    }
    report
}

/// A unique `O_CREAT|O_EXCL` temp file in `target`'s directory, ready to be
/// `persist`ed over it. Staging beside the destination is what makes the final
/// rename atomic (same filesystem) — a temp in `/tmp` would not be.
pub(super) fn temp_in_parent(target: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    match target.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(p) => tempfile::NamedTempFile::new_in(p),
        None => tempfile::NamedTempFile::new_in("."),
    }
}

/// The [`Transfer`] for one whole-file hash-group: the blob is either paid for
/// once (`downloaded` = the content bytes its fetch paid for this run, from
/// `bytes`, else the size of the single `Fetched`; `resumed` = the bytes it
/// resumed from an earlier run) or reused from an on-disk donor with no download
/// (`Deduped`, contributing 0 to `downloaded`) — a group never mixes the two,
/// since [`PullCtx::fetch_group`] takes one path or the other. Every materialized
/// copy — the canonical (`Fetched` or `Deduped`) plus each `Linked` duplicate
/// path — is a full file on disk (`reconstructed` = size × copies). A group with
/// nothing written (all skipped or failed) contributes nothing.
fn group_transfer(outcomes: &[EntryOutcome], bytes: Option<EntryBytes>) -> Transfer {
    let paid_size = outcomes.iter().find_map(|o| match o {
        EntryOutcome::Fetched(n) => Some(*n),
        _ => None,
    });
    let reused_size = outcomes.iter().find_map(|o| match o {
        EntryOutcome::Deduped(n) => Some(*n),
        _ => None,
    });
    match paid_size.or(reused_size) {
        Some(n) => {
            let copies = outcomes
                .iter()
                .filter(|o| {
                    matches!(
                        o,
                        EntryOutcome::Fetched(_) | EntryOutcome::Linked | EntryOutcome::Deduped(_)
                    )
                })
                .count();
            let copies = u64::try_from(copies).unwrap_or(u64::MAX);
            Transfer {
                downloaded: paid_size.map_or(0, |size| bytes.map_or(size, |b| b.paid)),
                reconstructed: n.saturating_mul(copies),
                resumed: paid_size.and(bytes).map_or(0, |b| b.resumed),
            }
        }
        None => Transfer::default(),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
