//! `decdn bundle pull` — fetch every blob a bundle manifest references into an
//! output directory over the paid `cdn/client/v1` path (issue #391).
//!
//! The manifest comes from a local file (`-i`) or is fetched first by its own
//! BLAKE3 hash (`--hash`); either way entries are then fetched per *distinct*
//! blob hash. Entries naming the same blob (one file at two paths) are fetched —
//! and paid for — once and hard-linked (or copied) to each path (#1306). Node selection is
//! per blob (#936): with an explicit `--node-id` every entry is pulled from that
//! one node, otherwise each distinct blob discovers its own holder among the
//! region-nearest active nodes. Every group's fetch — a plain whole-file blob,
//! or a hint-carrying entry's pay-now-range fetch — fans out concurrently,
//! bounded by one global `--jobs` cap (`PullCtx.gate`). An entry that only
//! waits for a sibling's donor chunks holds no slot while it waits. A manifest `chunks`
//! entry is never fetched or stored as its own blob: its hints only let a
//! byte range shared with another entry be recognized and spliced from disk
//! instead of paid for again.
//!
//! **Shared chunks are fetched once.** When a chunk hint hash appears in two or
//! more entries, the run assigns that chunk to one entry — its smallest holder,
//! computed up front from the whole manifest by `build_fetch_plan` — and only
//! that entry pays to fetch it. Groups are scheduled smallest-first, so a holder
//! starts before the larger entries that share its chunks. Each entry drives its
//! own ranges (unique chunks, chunks assigned to it, and the partial groups at
//! the two ends of each spliced run) up front and defers the rest; at its tail it splices each deferred range as the
//! assigned holder registers it, waiting on the holder — not a clock — and driving
//! a deferred range itself only if that holder finishes without producing it.
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
//! silently kept. Every skipped or freshly written path also seeds its
//! on-disk byte ranges as chunk donors, so a later run's or bundle's
//! complement-range fetch can splice an unchanged range from disk instead of
//! paying for it again.
//!
//! **Whole-root content reuse.** A run also reuses a byte-identical file
//! already anywhere in the output root, not only at its own manifest path. It
//! consults a whole-root content index built from `.decdn-manifest.json`'s
//! saved records: target blob hash → an on-disk regular file the manifest
//! records with that hash. When a blob's hash hits that index, the candidate
//! is confirmed by a whole-file re-hash, then the destination is materialized
//! by hard link (or copy) with no download and no payment. A file that only
//! partially matches an on-disk candidate still splices its common byte
//! ranges from disk the same way, through the donor mechanism described
//! above.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::IsTerminal as _;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context as _, anyhow, bail};
use decdn_common::cli::{BundlePullArgs, ClientFetchArgs};
use decdn_common::config::load_file_config;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity::{PasswordUse, load_signer};
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use futures_util::StreamExt as _;
use iroh::{Endpoint, PublicKey, RelayUrl};
use serde::{Deserialize, Serialize};

use super::bundle_cache;
use super::bundle_manifest::{self, SavedManifest, SavedMtime};
use super::buyer_store::open_client_store_for_buy;
use super::chain_ctx;
use super::fetch;
use super::interrupt::{Interrupt, Interrupted};
use super::manifest::build_glob_set;
use super::ordered_writes::OrderedWrites;
use super::pull_progress::{self, PullProgress};
use decdn_bao_range::CHUNK_GROUP_BYTES;
use decdn_client::discovery::{self, NodeCandidate};
use decdn_client::endpoint as client_endpoint;
use decdn_client::provider;
use decdn_client::{
    ClientRangedStore, Connections, DownloadTarget, Downloader, LaneLedgers, PeerHealth,
    ProgressCallback, ProgressClock, PullDeadlines, StopPolicy,
};

use super::cli_sources::CliSources;

type FetchTarget = (PublicKey, Address);

/// Group manifest entries by their blob `hash`, preserving first-seen order both
/// across groups and within each group. Entries that name the same blob (one
/// file published at two paths) land in one group so it is fetched once (#1306).
fn group_by_hash<'a>(entries: &[&'a ManifestEntry]) -> Vec<HashGroup<'a>> {
    let mut index: HashMap<&str, usize> = HashMap::new();
    let mut groups: Vec<HashGroup<'a>> = Vec::new();
    for entry in entries {
        let next = groups.len();
        let at = *index.entry(entry.hash.as_str()).or_insert(next);
        if at == next {
            groups.push(HashGroup {
                hash: entry.hash.as_str(),
                entries: vec![*entry],
            });
        } else if let Some(group) = groups.get_mut(at) {
            group.entries.push(*entry);
        }
    }
    groups
}

/// Order hash-groups smallest whole-file first so a shared chunk's assigned
/// fetcher (the smallest containing entry, per [`build_fetch_plan`]) starts — and
/// finishes — before the larger entries that defer to it, keeping the tail-reconcile
/// wait short. A group's entries share one blob, so the group's size is any entry's
/// declared `size` (the same OR-across-same-hash-entries rule as
/// [`total_content_bytes`], so a group with one unsized duplicate path still sorts
/// by its real size); a group no entry sizes sorts last (it can never be a sized
/// donor). Ties break on the group hash for a deterministic order.
fn order_groups_smallest_first(mut groups: Vec<HashGroup<'_>>) -> Vec<HashGroup<'_>> {
    groups.sort_by(|a, b| {
        let a_size = a.entries.iter().find_map(|e| e.size);
        let b_size = b.entries.iter().find_map(|e| e.size);
        // A group no entry sizes sorts last: map to the max sentinel for the compare.
        let key = |s: Option<u64>| s.unwrap_or(u64::MAX);
        key(a_size)
            .cmp(&key(b_size))
            .then_with(|| a.hash.cmp(b.hash))
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

/// [`link_or_copy_atomic`] on the blocking pool: its copy fallback moves a whole
/// blob.
async fn link_off_runtime(src: PathBuf, dest: PathBuf) -> anyhow::Result<()> {
    off_runtime("link", move || link_or_copy_atomic(&src, &dest)).await
}

/// Materialize `src`'s content at `dest` without re-reading it over the network:
/// a hard link where the filesystem allows it, else a full copy (cross-device
/// `EXDEV`, or a filesystem that can't link). Staged in `dest`'s parent and
/// renamed into place so `dest` is only ever absent or complete — the same
/// atomic-replace invariant [`materialize`] upholds, which [`resolve_disk_state`]
/// and [`plan_slots`] rely on when deciding a path is skip-safe.
fn link_or_copy_atomic(src: &Path, dest: &Path) -> anyhow::Result<()> {
    let parent = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // A duplicate path may live in a subdir the canonical path never created.
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let staged = tempfile::Builder::new()
        .prefix(".decdn-link-")
        .make_in(parent, |candidate| {
            match std::fs::hard_link(src, candidate) {
                Ok(()) => Ok(()),
                // A fresh random candidate colliding is make_in's signal to retry.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(e),
                // EXDEV or an unsupported link: fall back to a full byte copy.
                Err(_) => std::fs::copy(src, candidate).map(|_| ()),
            }
        })
        .with_context(|| {
            format!(
                "stage link/copy of {} for {}",
                src.display(),
                dest.display()
            )
        })?;
    staged
        .persist(dest)
        .map_err(|e| e.error)
        .with_context(|| format!("write {}", dest.display()))?;
    Ok(())
}

/// A disk-seeded chunk donor: a byte range already on disk whose bytes back a
/// chunk `hash`. Fed into [`ChunkIndex`] before fetching so a splice can reuse
/// on-disk bytes across runs. The source is an OUTPUT path (not a staging blob),
/// verified by the existing per-chunk re-hash before any splice trusts it.
struct SeedDonor {
    /// The chunk's BLAKE3 hash — the [`ChunkIndex`] key.
    hash: [u8; 32],
    /// The on-disk output file that holds the chunk's bytes.
    source: PathBuf,
    /// Byte offset of the chunk within `source`.
    offset: u64,
    /// Chunk length in bytes.
    len: u64,
}

/// The result of inspecting the output tree against the new manifest and the
/// saved skip-cache before fetching: which in-scope paths to skip, and the
/// on-disk chunk donors to seed.
#[derive(Default)]
struct DiskState {
    /// In-scope manifest paths whose on-disk file already matches the new
    /// manifest hash (fast-skip or re-hash-confirmed) — not fetched.
    skip: HashSet<String>,
    /// On-disk chunk donors to seed into the run's [`ChunkIndex`].
    seed: Vec<SeedDonor>,
    /// Whole-file donors already on disk anywhere in the root: target blob hash
    /// → an on-disk regular file the saved manifest records with that hash. A hit
    /// lets a group materialize by link/copy instead of fetching — but only after
    /// the candidate is re-hashed and confirmed (done at the use site).
    whole_file: HashMap<[u8; 32], PathBuf>,
}

/// Classify every in-scope entry against the output tree and the saved
/// skip-cache: fast-skip on a matching saved record (hash + size + mtime), else
/// re-hash the on-disk bytes against the new manifest hash, else fetch. With
/// `overwrite` nothing is skipped. The whole-file `hash` is authoritative, so a
/// stale or absent saved record only costs a re-hash, never correctness.
async fn resolve_disk_state(
    entries: &[ManifestEntry],
    saved: &SavedManifest,
    out_root: &Path,
    overwrite: bool,
) -> DiskState {
    let mut state = DiskState::default();
    if overwrite {
        return state;
    }
    for en in entries {
        let Ok(dest) = safe_join(out_root, &en.path) else {
            continue; // a bad path fails later in plan_slots; not skippable
        };
        if dest.starts_with(out_root.join(STAGING_DIR)) {
            continue;
        }
        // `symlink_metadata` does not follow links: a symlink or any
        // non-regular file is never skipped, re-hashed, or seeded as a donor —
        // it is left to the fetch path, whose atomic materialize replaces it
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
        // a first claim, and a blob whose true size differs from it is still
        // the entry's blob, so it is not fetched again on every run.
        let fast = saved.get(&en.path).is_some_and(|rec| {
            rec.hash == en.hash
                && rec.size == meta.len()
                && SavedMtime::of(&meta).as_ref() == Some(&rec.mtime)
        });
        if fast {
            state.skip.insert(en.path.clone());
            seed_new_chunks(&mut state, en, &dest);
            continue;
        }
        // Re-hash gate: confirm the on-disk bytes against the new manifest hash.
        let Ok(want) = fetch::parse_hash(&en.hash) else {
            continue;
        };
        let dest_buf = dest.clone();
        let got = tokio::task::spawn_blocking(move || hash_partial(&dest_buf)).await;
        if let Ok(Ok(got)) = got
            && got == want
        {
            state.skip.insert(en.path.clone());
            seed_new_chunks(&mut state, en, &dest);
            continue;
        }
        // Present but mismatched: this path will be fetched. Its current bytes
        // are the OLD file and survive on disk until this entry's own group
        // atomically materializes (temp + rename), so they are a safe donor for
        // any other entry that shares an old chunk in the meantime.
        if let Some(rec) = saved.get(&en.path)
            && let Some(old) = bundle_manifest::saved_hints(rec)
        {
            for (chash, offset, len) in old {
                if let Ok(h) = fetch::parse_hash(&chash) {
                    state.seed.push(SeedDonor {
                        hash: h,
                        source: dest.clone(),
                        offset,
                        len,
                    });
                }
            }
        }
    }
    // Widen reuse to the whole root: every saved record whose file still exists
    // as a regular file is a donor keyed by content — a whole-file donor (for a
    // no-download link, verified by re-hash at the use site) and a chunk donor
    // (spliced under the existing per-chunk re-hash guard). This is what lets a
    // file shared across bundles at different paths be reused.
    //
    // A path this run will overwrite (an in-scope entry not already in
    // `state.skip`) is excluded from `whole_file` only: its bytes can be
    // atomically replaced by its own group between the whole-file link's
    // re-hash and its link/copy (TOCTOU), which would silently corrupt the
    // link's destination with no post-link verification to catch it. Chunk
    // donors from the same record stay in `state.seed` — a spliced range is
    // always covered by the final whole-file re-verification, so the race
    // there is harmless.
    let will_write: HashSet<&str> = entries
        .iter()
        .map(|e| e.path.as_str())
        .filter(|p| !state.skip.contains(*p))
        .collect();
    for (path, rec) in saved.records() {
        let Ok(src) = safe_join(out_root, path) else {
            continue;
        };
        if src.starts_with(out_root.join(STAGING_DIR)) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&src) else {
            continue; // gone / unreadable
        };
        if !meta.is_file() {
            continue; // symlink / non-regular → never a donor
        }
        if !will_write.contains(path)
            && let Ok(h) = fetch::parse_hash(&rec.hash)
        {
            state.whole_file.entry(h).or_insert_with(|| src.clone());
        }
        if let Some(hints) = bundle_manifest::saved_hints(rec) {
            for (chash, offset, len) in hints {
                if let Ok(h) = fetch::parse_hash(&chash) {
                    state.seed.push(SeedDonor {
                        hash: h,
                        source: src.clone(),
                        offset,
                        len,
                    });
                }
            }
        }
    }
    state
}

/// Seed `state.seed` with the NEW manifest entry's chunk donors, sourced from
/// `dest` — the entry's own output file, confirmed unchanged (skipped) this
/// run. Seeding never gates skip/fetch; it only supplies optional splice
/// donors for other entries.
fn seed_new_chunks(state: &mut DiskState, en: &ManifestEntry, dest: &Path) {
    if let Some(hints) = hints_of(en) {
        for h in hints {
            state.seed.push(SeedDonor {
                hash: h.hash,
                source: dest.to_path_buf(),
                offset: h.offset,
                len: h.len,
            });
        }
    }
}

/// Resolve each entry's on-disk destination and classify it — a resolve failure,
/// a path to skip, or a path to write — before any fetch. `skip` is the
/// [`resolve_disk_state`] pre-pass result: a path lands in it on a matching
/// saved-manifest record (fast-skip) or, failing that, on a re-hash of the
/// on-disk bytes against the new manifest hash. A path whose content changed
/// is written, not silently kept. Evaluated **per destination**, so one path
/// of a duplicated blob can be skipped while another is written. With
/// `overwrite` set, `skip` is empty and every destination is written.
fn plan_slots<'a>(
    entries: &[&'a ManifestEntry],
    out_root: &Path,
    overwrite: bool,
    skip: &HashSet<String>,
) -> Vec<Slot<'a>> {
    entries
        .iter()
        .map(|en| match safe_join(out_root, &en.path) {
            Err(e) => Slot::Failed(EntryOutcome::failed(&en.path, &e)),
            // Reserve the staging dir: an entry resolving inside
            // `<out_root>/.decdn-partial/` would collide with a per-hash staging
            // file, and `remove_staging` could then delete a materialized output.
            Ok(dest) if dest.starts_with(out_root.join(STAGING_DIR)) => {
                Slot::Failed(EntryOutcome::failed(
                    &en.path,
                    &anyhow::anyhow!(
                        "manifest path {:?} is inside the reserved staging directory {STAGING_DIR}/",
                        en.path
                    ),
                ))
            }
            Ok(_) if !overwrite && skip.contains(en.path.as_str()) => Slot::Skip,
            Ok(dest) => Slot::Write {
                label: en.path.as_str(),
                dest,
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
        let got = hash_partial(&target).ok()?;
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
    slots: Vec<Slot<'_>>,
    donor: &Path,
    size: u64,
) -> Vec<EntryOutcome> {
    materialize_group(
        slots,
        |dest| {
            let donor = donor.to_path_buf();
            async move { link_off_runtime(donor, dest).await.map(|()| size) }
        },
        link_off_runtime,
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

/// Read-side bundle manifest (the write-side lives in [`super::bundle`]). `size`
/// is optional on the wire (per `appendix-bundles.md`); it is informational for
/// the dry-run plan and not required to fetch.
#[derive(Debug, Deserialize)]
struct Manifest {
    version: u32,
    entries: Vec<ManifestEntry>,
}

/// A manifest cut down to the entries this run pulls, and how many entries
/// the `--include`/`--exclude` filter and the `--select` editor dropped.
#[derive(Debug)]
struct Kept {
    manifest: Manifest,
    /// Entries the run does not pull: filtered out or deselected.
    excluded: u64,
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    path: String,
    hash: String,
    #[serde(default)]
    size: Option<u64>,
    /// Optional ordered chunk decomposition (a range-dedup hint, per
    /// `appendix-bundles.md`). The whole file is always fetched and verified by
    /// the authoritative whole-file `hash`; when this list is present its chunk
    /// `(hash, size)` pairs let a fetch splice byte ranges already materialized
    /// on disk by a sibling entry that shares a chunk, and pay only for the
    /// complement. When absent the file is fetched as one blob by `hash`.
    #[serde(default)]
    chunks: Option<Vec<ManifestChunk>>,
}

/// One chunk of a hint-carrying [`ManifestEntry`]: an independently
/// BLAKE3-addressed run of the file's bytes. Both fields are read — `hash`
/// identifies the chunk in the in-run [`ChunkIndex`], and `size` places it at a
/// byte offset (the running sum of prior chunk sizes) and bounds the range a
/// sibling may splice.
#[derive(Debug, Deserialize)]
struct ManifestChunk {
    /// The chunk's BLAKE3 content address (`b3:`hex).
    hash: String,
    /// The chunk's length in bytes; the chunk sizes sum to the entry's
    /// whole-file `size`.
    size: u64,
}

/// The run's whole-download content size — the fixed denominator for the total
/// progress bar. Every entry declares its whole-file `size` and is deduped by
/// `hash` (a blob fetched once and materialized to several paths counts once,
/// matching the fetch-once grouping), whether or not it carries range-dedup
/// chunk hints. A blob whose manifest `size` is absent contributes nothing,
/// exactly as it then moves the total bar not at all, so the numerator and
/// denominator stay consistent. `None` when nothing kept declares a size — the
/// total bar is then omitted and only per-file bars render.
fn total_content_bytes(entries: &[ManifestEntry]) -> Option<u64> {
    // Entries keyed by whole-file hash, OR-ing in a declared size wherever one of
    // the same-hash entries carries it (the file bar picks its size the same way).
    let mut by_hash: HashMap<&str, Option<u64>> = HashMap::new();
    for entry in entries {
        let slot = by_hash.entry(entry.hash.as_str()).or_insert(None);
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

/// One blob's static **download** (pay-now) bytes and its **reconstruct**
/// (spliced-from-disk) bytes, planned against `index` under the run's
/// [`FetchPlan`]: `download` is the sum of the blob's drive ranges (its unique and
/// self-assigned chunks — a shared chunk it defers to a sibling is spliced, not
/// downloaded) and `reconstruct` is the rest of its declared `size`. A plain
/// (unhinted) blob downloads its whole `size` and reconstructs nothing. Returns
/// `(0, 0)` when the blob declares no size. Passing an empty `index` gives the
/// fresh-pull figures; a resumed run's pre-seeded donors only shrink `download`.
fn blob_download_reconstruct(
    group: &HashGroup<'_>,
    fetch_plan: &FetchPlan,
    index: &HashMap<[u8; 32], MaterializedRange>,
) -> (u64, u64) {
    let Some(total) = group.entries.iter().find_map(|e| e.size) else {
        return (0, 0);
    };
    let hints = group.entries.first().and_then(|e| hints_of(e));
    let whole = group
        .entries
        .first()
        .and_then(|e| fetch::parse_hash(&e.hash).ok());
    let download = match (hints, whole) {
        (Some(hints), Some(whole)) => plan_reassembly(&hints, index, fetch_plan, whole, total)
            .drive
            .iter()
            .map(|r| r.1)
            .fold(0u64, u64::saturating_add),
        // No usable hints (or an unparseable hash) → the whole blob is downloaded.
        _ => total,
    };
    (download, total.saturating_sub(download))
}

/// The run's static **download** total — the content bytes it pays to fetch, deduped
/// by whole-file `hash` and by shared chunk (each shared chunk counted once, under
/// its assigned fetcher). This is the total progress bar's denominator; the run
/// finishes downloading exactly these bytes and splices the rest from disk. `None`
/// when nothing kept declares a size (no total bar). Computed against an empty index
/// (the fresh-pull figure), so a resumed run downloads at most this.
/// The run label for the progress header: the output directory's final component,
/// or its whole path when it has none (a bare root).
fn run_label(output: &Path) -> String {
    output.file_name().map_or_else(
        || output.to_string_lossy().into_owned(),
        |n| n.to_string_lossy().into_owned(),
    )
}

fn download_bytes(entries: &[ManifestEntry], fetch_plan: &FetchPlan) -> Option<u64> {
    let refs: Vec<&ManifestEntry> = entries.iter().collect();
    let empty = HashMap::new();
    let mut sum = 0u64;
    let mut any = false;
    for group in group_by_hash(&refs) {
        if group.entries.iter().find_map(|e| e.size).is_none() {
            continue;
        }
        any = true;
        let (download, _) = blob_download_reconstruct(&group, fetch_plan, &empty);
        sum = sum.saturating_add(download);
    }
    any.then_some(sum)
}

/// The compiled `--include`/`--exclude` globs that select which manifest entries
/// a pull run fetches. Both sets match an entry's POSIX relative `path` — the
/// manifest field (`models/a.bin`), never the on-disk absolute path — with the
/// same gitignore glob dialect `origin import --exclude` uses (`*` does not
/// cross `/`, `**` recurses), via [`build_glob_set`].
///
/// An entry is kept iff it passes the include gate AND matches no exclude. The
/// include gate is open when no `--include` was given (every entry passes) and
/// otherwise requires a match against at least one include pattern; `--exclude`
/// always wins over `--include`.
///
/// Every pattern matches the whole path from the bundle root, so `metal/*`
/// matches `metal/model.bin` but not `gpt/metal/model.bin`; `**/metal/*`
/// matches both. A pattern that matches no entry is almost always this mistake,
/// so [`Self::apply_and_warn`] names each one.
#[derive(Debug)]
struct EntryFilter {
    include: globset::GlobSet,
    /// The `--include` patterns as given, in the order [`build_glob_set`]
    /// compiled them, so a [`globset::GlobSet::matches`] index names its
    /// pattern.
    include_patterns: Vec<String>,
    exclude: globset::GlobSet,
    /// The `--exclude` patterns as given, indexed like `include_patterns`.
    exclude_patterns: Vec<String>,
}

/// A `--include` or `--exclude` pattern that matched no entry of the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Unmatched {
    /// The flag the pattern came from: `--include` or `--exclude`.
    flag: &'static str,
    /// The pattern as given.
    pattern: String,
    /// The same pattern under a leading `**/`, when that form matches at least
    /// one entry: the likely intent of a pattern written as if it matched at
    /// any depth.
    suggestion: Option<String>,
}

impl Unmatched {
    /// The warning line for this pattern, without the `warning: ` prefix.
    fn warning(&self) -> String {
        let hint = self
            .suggestion
            .as_ref()
            .map_or_else(String::new, |suggestion| {
                format!(
                    " (a pattern matches the whole path from the bundle root; did you mean \
                     '{suggestion}'?)"
                )
            });
        format!("{} '{}' matched no entries{hint}", self.flag, self.pattern)
    }
}

/// What [`EntryFilter::apply_reporting`] hands back.
#[derive(Debug)]
struct FilterPass {
    /// The entries the filter keeps, in manifest order.
    kept: Vec<ManifestEntry>,
    /// How many entries the filter dropped.
    excluded: u64,
    /// Each pattern that matched no entry, `--include` ones first.
    unmatched: Vec<Unmatched>,
}

impl EntryFilter {
    /// Compile the run's `--include`/`--exclude` patterns. A malformed glob is a
    /// hard error naming the flag it came from.
    fn compile(include: &[String], exclude: &[String]) -> anyhow::Result<Self> {
        Ok(Self {
            include: build_glob_set(include, "--include")?,
            include_patterns: include.to_vec(),
            exclude: build_glob_set(exclude, "--exclude")?,
            exclude_patterns: exclude.to_vec(),
        })
    }

    /// Retain only the entries the filter keeps, preserving manifest order, and
    /// report the patterns that matched no entry. A pattern's hits are counted
    /// over every entry of `entries`, before either gate drops any, so an
    /// `--exclude` that matches only entries the include gate already dropped
    /// still counts as a match.
    fn apply_reporting(&self, entries: Vec<ManifestEntry>) -> FilterPass {
        let mut include_hits = vec![false; self.include_patterns.len()];
        let mut exclude_hits = vec![false; self.exclude_patterns.len()];
        let keep: Vec<bool> = entries
            .iter()
            .map(|e| {
                let p = Path::new(&e.path);
                let included = mark_hits(&self.include, p, &mut include_hits);
                let excluded = mark_hits(&self.exclude, p, &mut exclude_hits);
                (self.include_patterns.is_empty() || included) && !excluded
            })
            .collect();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        let unmatched = unmatched_patterns("--include", &self.include_patterns, &include_hits)
            .chain(unmatched_patterns(
                "--exclude",
                &self.exclude_patterns,
                &exclude_hits,
            ))
            .map(|mut u| {
                u.suggestion = anywhere_suggestion(&u.pattern, &paths);
                u
            })
            .collect();
        let raw = entries.len();
        let kept: Vec<ManifestEntry> = entries
            .into_iter()
            .zip(keep)
            .filter_map(|(e, keep)| keep.then_some(e))
            .collect();
        let excluded = u64::try_from(raw.saturating_sub(kept.len())).unwrap_or(u64::MAX);
        FilterPass {
            kept,
            excluded,
            unmatched,
        }
    }

    /// [`Self::apply_reporting`], with one `warning:` line on stderr for each
    /// pattern that matched no entry. Returns the kept entries and how many the
    /// filter dropped.
    fn apply_and_warn(&self, entries: Vec<ManifestEntry>) -> (Vec<ManifestEntry>, u64) {
        let pass = self.apply_reporting(entries);
        for u in &pass.unmatched {
            eprintln!("warning: {}", u.warning());
        }
        (pass.kept, pass.excluded)
    }
}

/// Mark in `hits` every pattern of `set` that matches `path`, and return
/// whether any did.
fn mark_hits(set: &globset::GlobSet, path: &Path, hits: &mut [bool]) -> bool {
    let matched = set.matches(path);
    for i in &matched {
        if let Some(hit) = hits.get_mut(*i) {
            *hit = true;
        }
    }
    !matched.is_empty()
}

/// The patterns of `flag` whose `hits` slot is unset, with no suggestion yet.
fn unmatched_patterns<'a>(
    flag: &'static str,
    patterns: &'a [String],
    hits: &'a [bool],
) -> impl Iterator<Item = Unmatched> + 'a {
    patterns
        .iter()
        .zip(hits)
        .filter(|(_, hit)| !**hit)
        .map(move |(pattern, _)| Unmatched {
            flag,
            pattern: pattern.clone(),
            suggestion: None,
        })
}

/// `**/<pattern>`, when `pattern` does not start with `**` or `/` and that form
/// matches at least one of `paths`.
fn anywhere_suggestion(pattern: &str, paths: &[&str]) -> Option<String> {
    if pattern.starts_with("**") || pattern.starts_with('/') {
        return None;
    }
    let anywhere = format!("**/{pattern}");
    let set = build_glob_set(std::slice::from_ref(&anywhere), "suggestion").ok()?;
    paths
        .iter()
        .any(|p| set.is_match(Path::new(p)))
        .then_some(anywhere)
}

/// Header written above the file list in the `--select` editor buffer. Explains
/// the git-rebase-style convention: commented or deleted lines are skipped.
const SELECT_HEADER: &str = "\
# decdn bundle pull --select — choose which files to pull.
#
# Comment out (prefix with #) or delete any line to SKIP that file.
# Save and exit to pull every file still listed below; delete them all to pull
# nothing. Reordering has no effect, and you cannot add files here.
#
# Each line is  <path>\t<size>  — only the path (before the tab) is read.
";

/// Render the `--select` editor buffer: the header, then one line per entry as
/// `<path>\t<human size>`. The size is a right-hand annotation for context only;
/// [`parse_selection`] reads just the path before the first tab.
fn render_selection(entries: &[ManifestEntry]) -> String {
    let mut out = String::from(SELECT_HEADER);
    for e in entries {
        let size = e.size.map_or_else(|| "?".to_string(), human_bytes);
        out.push_str(&e.path);
        out.push('\t');
        out.push_str(&size);
        out.push('\n');
    }
    out
}

/// Reject a bundle whose paths cannot round-trip through the `--select` editor
/// buffer exactly. [`parse_selection`] reads a kept line as the text before the
/// first tab, trimmed, and skips a line whose first non-whitespace character is
/// `#`. A path therefore fails to round-trip when it:
/// - differs from its trimmed form (leading/trailing whitespace, which
///   [`parse_selection`] strips — including whitespace before a `#`, which would
///   make the line read as a comment and silently drop the file), or
/// - has `#` as its first non-whitespace character (read as a comment), or
/// - contains a tab, newline, or carriage return (splits or breaks its line).
///
/// Such paths are pathological for a POSIX relative filename; `--select` refuses
/// them loudly rather than silently dropping a file the user meant to keep.
/// `--include`/`--exclude` still handle these bundles.
fn check_selectable(entries: &[ManifestEntry]) -> anyhow::Result<()> {
    if let Some(bad) = entries.iter().find(|e| {
        let p = e.path.as_str();
        p != p.trim() || p.trim_start().starts_with('#') || p.contains(['\t', '\n', '\r'])
    }) {
        bail!(
            "--select cannot represent the path {:?} (paths with leading/trailing \
             whitespace, a leading '#', or a tab/newline are unsupported); use \
             --include/--exclude instead",
            bad.path
        );
    }
    Ok(())
}

/// Apply the edited `--select` buffer back onto the entry list: keep only entries
/// whose path still appears on a non-comment, non-blank line, preserving the
/// original manifest order. A kept line whose path matches no bundle entry is a
/// hard error — the editor only removes files, so an unmatched line is a typo,
/// not an addition, and silently dropping it would be worse than failing.
fn parse_selection(
    edited: &str,
    entries: Vec<ManifestEntry>,
) -> anyhow::Result<Vec<ManifestEntry>> {
    let known: HashSet<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    let mut kept: HashSet<String> = HashSet::new();
    let mut unknown: Vec<&str> = Vec::new();
    for line in edited.lines() {
        if line.trim_start().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        // The path is the text before the size annotation (first tab), trimmed.
        let path = line.split_once('\t').map_or(line, |(p, _)| p).trim();
        if path.is_empty() {
            continue;
        }
        if known.contains(path) {
            kept.insert(path.to_string());
        } else {
            unknown.push(path);
        }
    }
    if !unknown.is_empty() {
        bail!(
            "--select: these lines match no bundle entry (editing only removes files, \
             it cannot add them): {}",
            unknown.join(", ")
        );
    }
    Ok(entries
        .into_iter()
        .filter(|e| kept.contains(&e.path))
        .collect())
}

/// Apply the interactive `--select` step to a finalized manifest, or pass it
/// through unchanged when `--select` is off. Each deselected entry adds to the
/// excluded count. `Ok(None)` means the user deselected every file:
/// [`report_nothing_to_fetch`] was already called, so the run is done.
fn maybe_select(args: &BundlePullArgs, mut kept: Kept) -> anyhow::Result<Option<Kept>> {
    if args.select {
        kept = apply_selection(kept, select_entries)?;
        if kept.manifest.entries.is_empty() {
            report_nothing_to_fetch(NothingReason::Deselected);
            warn_leftover_partials(&args.output, &[], args.hash.as_deref());
            return Ok(None);
        }
    }
    Ok(Some(kept))
}

/// Replace `kept`'s entries with the ones `select` keeps, and add each entry it
/// drops to the excluded count.
fn apply_selection(
    mut kept: Kept,
    select: impl FnOnce(Vec<ManifestEntry>) -> anyhow::Result<Vec<ManifestEntry>>,
) -> anyhow::Result<Kept> {
    let offered = kept.manifest.entries.len();
    kept.manifest.entries = select(kept.manifest.entries)?;
    let dropped = offered.saturating_sub(kept.manifest.entries.len());
    kept.excluded = kept
        .excluded
        .saturating_add(u64::try_from(dropped).unwrap_or(u64::MAX));
    Ok(kept)
}

/// Reject a `--select` invocation that cannot work: it opens an editor, so it
/// needs an interactive terminal (both stdin and stdout) and is incompatible with
/// the non-interactive `--json` and `--dry-run` modes. A no-op when `--select` is
/// not set.
fn check_select_flags(args: &BundlePullArgs) -> anyhow::Result<()> {
    if !args.select {
        return Ok(());
    }
    if args.json {
        bail!("--select cannot be combined with --json (it needs an interactive editor)");
    }
    if args.dry_run {
        bail!("--select cannot be combined with --dry-run (it needs an interactive editor)");
    }
    if !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        bail!("--select needs an interactive terminal; use --include/--exclude instead");
    }
    Ok(())
}

/// Run the interactive `--select` step: render the entry list, open it in the
/// user's editor, and apply the edited buffer back. All decision logic lives in
/// the tested [`render_selection`]/[`parse_selection`]/[`check_selectable`]; this
/// only wires them to the editor. The caller has already verified an interactive
/// terminal and that `--json`/`--dry-run` are not set.
fn select_entries(entries: Vec<ManifestEntry>) -> anyhow::Result<Vec<ManifestEntry>> {
    check_selectable(&entries)?;
    let edited = edit_in_editor(&render_selection(&entries))?;
    parse_selection(&edited, entries)
}

/// Resolve the editor command from `$VISUAL`/`$EDITOR` into program + arguments,
/// falling back to `vi`. `$VISUAL` wins over `$EDITOR`; a missing or
/// blank/whitespace-only value is treated as unset. The value may carry arguments
/// (`code --wait`): it is split on ASCII whitespace, with no shell quoting or
/// escaping — an argument cannot itself contain a space. The scratch file path is
/// appended by the caller, not here. Never empty — the fallback guarantees at
/// least `["vi"]`.
fn editor_command(visual: Option<&str>, editor: Option<&str>) -> Vec<String> {
    let chosen = [visual, editor]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .unwrap_or("vi");
    chosen.split_whitespace().map(String::from).collect()
}

/// Open `buffer` in the user's editor and return the saved contents. The editor
/// is `$VISUAL`, then `$EDITOR`, then `vi`; the value may carry arguments
/// (`code --wait`), split on ASCII whitespace with no shell quoting (see
/// [`editor_command`]). The buffer is staged in a temporary file that is removed
/// when this returns.
fn edit_in_editor(buffer: &str) -> anyhow::Result<String> {
    let mut file = tempfile::Builder::new()
        .prefix("decdn-select-")
        .suffix(".txt")
        .tempfile()
        .context("create --select scratch file")?;
    std::io::Write::write_all(file.as_file_mut(), buffer.as_bytes())
        .context("write --select scratch file")?;
    file.as_file()
        .sync_all()
        .context("sync --select scratch file")?;

    let command = editor_command(
        std::env::var("VISUAL").ok().as_deref(),
        std::env::var("EDITOR").ok().as_deref(),
    );
    let (program, rest) = command
        .split_first()
        .ok_or_else(|| anyhow!("empty editor command"))?;

    let status = std::process::Command::new(program)
        .args(rest)
        .arg(file.path())
        .status()
        .with_context(|| format!("launch editor {program:?}"))?;
    if !status.success() {
        bail!("editor {program:?} exited with {status}; aborting --select");
    }

    std::fs::read_to_string(file.path()).context("read back --select scratch file")
}

/// A group of manifest entries that all name the same blob `hash` — one file
/// published at two (or more) bundle paths. Non-empty by construction; the
/// shared `hash` is carried explicitly so consumers never re-derive it from an
/// arbitrary member.
#[derive(Clone)]
struct HashGroup<'a> {
    hash: &'a str,
    entries: Vec<&'a ManifestEntry>,
}

/// Per-destination classification within a hash-group, resolved before any
/// fetch: a resolve failure, an already-present file to skip, or a path to
/// write. `label` (the entry's manifest-relative path) is retained only to tag
/// a later failure.
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
#[derive(Clone)]
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
    /// and `err` ends the whole pull when [`ends_the_pull`] says so.
    fn fetch_failed(slots: Vec<Slot<'_>>, err: anyhow::Error) -> Self {
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
fn entry_scope(err: &anyhow::Error) -> decdn_client::FatalScope {
    match decdn_client::classify(err) {
        decdn_client::Fault::Fatal(scope) => scope,
        _ => decdn_client::FatalScope::Item,
    }
}

/// Whether a failed entry fetch ends the whole pull: a command-scope fault
/// ([`entry_scope`]), or a give-up. A give-up is item-scoped, but every entry
/// shares the command's progress clock, so every other entry gives up at the
/// same moment.
fn ends_the_pull(err: &anyhow::Error) -> bool {
    entry_scope(err) == decdn_client::FatalScope::Command
        || err.downcast_ref::<decdn_client::GaveUp>().is_some()
}

/// One-line `--json` summary. `fetched`/`linked`/`skipped`/`failed` are entry
/// counts; `downloaded` is the content bytes this run fetched and paid for across
/// the distinct blobs fetched (a blob shared across several paths counts once,
/// #1306; a range-dedup blob counts only the bytes it did not splice from disk)
/// and `reconstructed` is the total bytes written to disk this run — they diverge
/// when one blob is materialized to several paths or when range-dedup spliced part
/// of a blob. `downloaded` is a content-size tally, not an exact on-wire
/// measurement: it excludes bao proof overhead.
///
/// `resumed_bytes` is the content bytes of a `.partial` prefix that an earlier,
/// interrupted run fetched and paid for, which this run resumed rather than
/// fetched again. It is never part of `downloaded`. A byte that this run also
/// spliced from disk counts only in `spliced_bytes`.
///
/// `excluded` counts the manifest entries this run does not pull: the ones the
/// `--include`/`--exclude` filter dropped and the ones deselected under
/// `--select`.
///
/// `reused` and `reused_bytes` report the whole-file dedup outcome, counted per
/// distinct blob exactly as `fetched`/`downloaded` are (a blob reused at several
/// paths counts once here; its extra destinations are `linked`): `reused` is the
/// count of distinct blobs materialized from an on-disk whole-file donor (verified
/// by re-hash before use), and `reused_bytes` sums those blobs' sizes once each —
/// bytes served from disk with no download and no payment.
///
/// `spliced_bytes` and `hints_ignored` report the range-dedup outcome so a run
/// whose hints saved bytes is distinguishable from one whose hints did not:
/// `spliced_bytes` is the total bytes served from a local donor splice (bytes NOT
/// downloaded or paid for), and `hints_ignored` counts range-dedup hints dropped
/// by a fault — a chunk set that failed to parse or whose sizes did not sum to the
/// file size, a donor that failed its verification re-hash (or was unreadable), or
/// a self-heal whole-file re-drive that discarded already-spliced donors. Both are
/// reporting only and never affect payment.
#[derive(Serialize)]
struct PullReport {
    output: String,
    fetched: u64,
    linked: u64,
    skipped: u64,
    /// Entries the filter or `--select` dropped.
    excluded: u64,
    failed: u64,
    downloaded: u64,
    reconstructed: u64,
    spliced_bytes: u64,
    hints_ignored: u64,
    /// Content bytes of `.partial` prefixes that earlier runs fetched and this
    /// run resumed; never part of `downloaded`.
    resumed_bytes: u64,
    /// Count of distinct blobs materialized from an on-disk whole-file donor;
    /// extra destinations of the same blob are counted in `linked`.
    reused: u64,
    /// Sum of those blobs' sizes (once per blob) — bytes materialized from a
    /// whole-file donor with no download or payment.
    reused_bytes: u64,
}

/// The run's range-dedup outcome, read off [`PullCtx::dedup_stats`] once the pull
/// finishes and folded into the [`PullReport`] and the human summary. See
/// [`PullReport`] for the exact meaning of each field.
#[derive(Clone, Copy, Default)]
struct DedupSummary {
    spliced_bytes: u64,
    hints_ignored: u64,
}

/// One dedup entry's range-dedup outcome, returned by [`reassemble_dedup`].
/// [`PullCtx::pull_entry_untimed`] accumulates its spliced bytes and ignored
/// hints into [`PullCtx::dedup_stats`] and folds all of it into the entry's
/// [`EntryBytes`].
#[derive(Clone, Copy, Default, Debug)]
struct DedupOutcome {
    /// Bytes this entry served from a local donor splice — never downloaded.
    spliced_bytes: u64,
    /// Bytes this entry's ranged store held from an earlier run before this run
    /// drove anything, less the bytes it spliced — neither downloaded nor
    /// spliced here.
    resumed_bytes: u64,
    /// Range-dedup hints this entry dropped by a fault (a donor that failed its
    /// verification re-hash or was unreadable, or a self-heal re-drive that
    /// discarded every already-spliced donor).
    hints_ignored: u64,
}

impl DedupOutcome {
    /// The outcome of an entry of `total` bytes whose net spliced spans are
    /// `spliced` and whose store held the `resumed` spans from an earlier run.
    /// A byte in both counts as spliced, so no byte counts twice.
    fn of(spliced: &[(u64, u64)], resumed: &[(u64, u64)], total: u64, hints_ignored: u64) -> Self {
        Self {
            spliced_bytes: span_bytes(spliced),
            resumed_bytes: span_bytes(&uncovered_runs(resumed, spliced, total)),
            hints_ignored,
        }
    }

    /// This outcome, or, when a drive fetched the whole blob again after its
    /// finalize failed the hash (`refetched`), one with nothing spliced or
    /// resumed. That pass dropped every byte the store held and fetched the
    /// blob in full, and it always ends the entry, so this run paid for all
    /// of it.
    const fn refetched_if(self, refetched: bool) -> Self {
        if refetched {
            Self {
                spliced_bytes: 0,
                resumed_bytes: 0,
                hints_ignored: self.hints_ignored,
            }
        } else {
            self
        }
    }
}

/// Run-scoped range-dedup counters, shared by every concurrent entry via
/// `&PullCtx`. Atomics because entries run concurrently under `buffer_unordered`;
/// `Relaxed` is enough — the totals are read once, after the pull joins.
#[derive(Default)]
struct DedupStats {
    spliced_bytes: AtomicU64,
    hints_ignored: AtomicU64,
}

/// A pull's byte accounting: `downloaded` is the content bytes this run fetched and
/// paid for across the distinct blobs fetched (a blob materialized to several paths
/// counts once, #1306; a range-dedup blob counts only the bytes it did not splice
/// from disk); `reconstructed` is the total bytes written to disk (every
/// materialized copy); `resumed` is the content bytes of `.partial` prefixes that
/// earlier runs fetched and this run resumed. `downloaded` is content bytes, not
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

/// The run's candidate list: every active registered node. Each entry draws its
/// own candidate sample from the whole list ([`entry_candidates`]), so no
/// registered node is hidden from every entry by one draw.
///
/// The peer store's identity-fresh records serve as that list when enough of
/// them qualify ([`cached_registry`]), and the run issues no
/// `getRegisteredNodes`. Otherwise the registry is read once. A cached list that
/// runs out of holders falls back to a live read through the in-run
/// rediscovery ([`CliSources`](super::cli_sources)).
async fn discover_candidates(
    chain: &fetch::ResolvedChain,
    common: &ClientFetchArgs,
) -> anyhow::Result<Vec<discovery::NodeCandidate>> {
    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow!(
            "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
             blockchain.capacity_bond_address), or pass --node-id to pull from one node"
        )
    })?;
    if !common.rediscover
        && let Some(cached) = cached_registry(&chain.data_dir, fetch::now_secs_cli())
    {
        return Ok(cached);
    }
    let registry_cap = common.discovery_cap();
    let bootstrap =
        discovery::bootstrap_nodes(&chain.rpc_url, capacity_bond, &chain.data_dir, registry_cap)
            .await?;
    // See `fetch::discover_provider`: the degraded bootstrap paths reach the
    // operator through the return value; surface it here at `warn`.
    if let Some(warning) = bootstrap.warning() {
        tracing::warn!("{warning}");
    }
    let all = bootstrap.into_peers();
    if all.is_empty() {
        bail!("no active nodes in the CapacityBond registry at {capacity_bond}");
    }
    Ok(all)
}

/// The peer store's identity-fresh records under `data_dir`, or `None` when
/// fewer than [`decdn_client::StoreConfig::min_fresh_candidates`] qualify. The
/// records are the last live registry read, confirmed within
/// [`decdn_client::StoreConfig::identity_refresh_secs`]. The run only reads
/// them: identity refreshes on a live registry read alone, so the refresh
/// horizon still expires.
fn cached_registry(data_dir: &Path, now_secs: u64) -> Option<Vec<NodeCandidate>> {
    let store = decdn_client::PeerStore::open(data_dir);
    let cfg = decdn_client::StoreConfig::default();
    let cached = fetch::identity_fresh_candidates(&store, &cfg, now_secs);
    (cached.len() >= cfg.min_fresh_candidates).then_some(cached)
}

/// One entry's probe candidates: a fresh sample of `registry`, same-region
/// nodes first ([`discovery::select_candidates`]). A sample per entry costs no
/// extra probe, because each entry probes its own candidates, and a node that
/// one draw leaves out can still reach other entries.
fn entry_candidates(registry: &[NodeCandidate], region: Option<&str>) -> Vec<NodeCandidate> {
    discovery::select_candidates(registry.to_vec(), region, discovery::SELECT_K)
}

/// Load the buyer's Ethereum signer (vouchers + any openPool/topUp tx) and its
/// address, prompting for the keystore password once. Password from
/// `$DECDN_KEYSTORE_PASSWORD`, else `--keystore-password-file`, else a TTY
/// prompt.
fn load_buyer_signer(
    chain: &fetch::ResolvedChain,
) -> anyhow::Result<(Arc<PrivateKeySigner>, Address)> {
    let password = super::chain_ctx::read_keystore_password(
        &super::chain_ctx::password_sources(
            chain.keystore_password_file.as_deref(),
            PasswordUse::Unlock,
        ),
        "eth keystore password",
    )?
    .into_secret();
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    let self_address = signer.address();
    Ok((signer, self_address))
}

/// The resolved node selection and buyer signer for a pull run.
struct Selection {
    /// `Some((node, provider))` pins every entry to one node; `None` discovers per
    /// entry against a sample of `registry`.
    explicit: Option<FetchTarget>,
    /// Every active registered node, resolved once for the run
    /// ([`discover_candidates`]).
    registry: Option<Vec<NodeCandidate>>,
    signer: Arc<PrivateKeySigner>,
    self_address: Address,
}

/// Resolve the node selection and buyer signer for a pull run.
///
/// The keystore prompt is deferred until AFTER the registry read on the
/// discovery path, so a failed discovery never prompts.
async fn resolve_selection(
    common: &ClientFetchArgs,
    chain: &fetch::ResolvedChain,
) -> anyhow::Result<Selection> {
    // Explicit single node for every entry, or the active set (cached or read
    // from `CapacityBond` once), which each entry samples.
    let explicit = explicit_target(common)?;
    let registry = match explicit {
        Some(_) => None,
        // A registry read is bounded by `--timeout-ms` (#1349), inside
        // `bootstrap_nodes` so a timeout still falls through to the peer store.
        // No sampling or probing happens here: each entry samples and probes
        // its own candidates (`PullCtx::entry_candidates`).
        None => Some(discover_candidates(chain, common).await?),
    };
    let (signer, self_address) = load_buyer_signer(chain)?;
    Ok(Selection {
        explicit,
        registry,
        signer,
        self_address,
    })
}

/// Entry point for `decdn bundle pull`.
pub async fn bundle_pull(args: &BundlePullArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    // Validate the flag combination first — BEFORE the dry-run short-circuit — so a
    // bad combo (e.g. `--provider-address` without `--node-id`) or an unusable
    // timeout pair is rejected even for `--dry-run`. This rule does not run at
    // parse time, so `bundle_pull` calls it BEFORE the dry-run short-circuit. It
    // is cheap and side-effect-free.
    args.common.validate()?;

    // Compile the `--include`/`--exclude` entry filter BEFORE the dry-run
    // short-circuit — same rationale as `validate()`: a malformed glob is
    // rejected before any network, chain, or keystore work, and `--dry-run`
    // reflects the filtered plan too.
    let filter = EntryFilter::compile(&args.include, &args.exclude)?;
    let filters_given = !args.include.is_empty() || !args.exclude.is_empty();

    // `--select` opens an editor: reject an incompatible combo or a non-terminal
    // up front — before any network, chain, or keystore work — so it fails fast
    // rather than after paying to fetch the manifest.
    check_select_flags(args)?;

    // Dry-run short-circuits before any network/chain/keystore activity.
    if args.dry_run {
        return dry_run(args, &filter, filters_given);
    }

    // A local manifest is read up front (no network) and filtered here, so a
    // bundle that is empty — or emptied by the filter — needs no endpoint or
    // keystore password at all.
    let local_manifest = match &args.input {
        Some(path) => {
            let mut m = read_local_manifest(path)?;
            let raw_empty = m.entries.is_empty();
            let (entries, excluded) = filter.apply_and_warn(m.entries);
            m.entries = entries;
            if m.entries.is_empty() {
                report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
                warn_leftover_partials(&args.output, &[], None);
                return Ok(());
            }
            Some(Kept {
                manifest: m,
                excluded,
            })
        }
        None => None,
    };

    let common = &args.common;
    let relays = client_endpoint::resolve_relays(common.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let file = load_file_config(config_path)?;
    let mut chain = fetch::resolve_chain(common, &file)?;

    // Delegated adoption: `--capability`/`--capability-file` names a pool the
    // caller does NOT own and a capability authorizing this client's key to
    // spend against it (every entry pulls from that one pool). `None` => the
    // unchanged self-owned pool path; a delegated grant also disables reactive
    // top-up in `chain`.
    let grant = fetch::resolve_delegation_grant(common, &mut chain)?;

    // Same guard as `decdn fetch`, for the same reason and before the same
    // password prompt: a node's data dir is a buy target only when the
    // operator named it (#2082).
    // Shared, so a lane's watermark write runs off the runtime thread.
    let store = Arc::new(open_client_store_for_buy(
        &chain.data_dir,
        chain.data_dir_source,
        "pull",
    )?);
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    // The command's peer-record and watermark writes, off the runtime thread.
    let writes = OrderedWrites::default();
    // The body runs in `pull_over`, so the endpoint closes on every exit —
    // success, an early return, or an error — and its open connections end
    // cleanly instead of being aborted on drop.
    let result = pull_over(
        args,
        &chain,
        grant,
        (&store, &writes),
        &endpoint,
        &relays,
        (&filter, filters_given),
        local_manifest,
    )
    .await;
    // On every exit, a Ctrl-C's included: what the entries queued is durable
    // before the command returns.
    writes.settle().await;
    endpoint.close().await;
    result
}

/// The part of [`bundle_pull`] that runs over its open `endpoint`: resolve the
/// providers and the buyer signer, obtain the manifest, and pull it. `filter` is
/// the compiled `--include`/`--exclude` filter and whether either flag was given.
#[allow(clippy::too_many_arguments)]
async fn pull_over(
    args: &BundlePullArgs,
    chain: &fetch::ResolvedChain,
    grant: Option<decdn_incentive::CapabilityGrant>,
    (store, writes): (&Arc<RedbBuyerPoolStore>, &OrderedWrites),
    endpoint: &Endpoint,
    relays: &[RelayUrl],
    (filter, filters_given): (&EntryFilter, bool),
    local_manifest: Option<Kept>,
) -> anyhow::Result<()> {
    let common = &args.common;
    // Selection + the buyer signer, resolved per path (see `resolve_selection`).
    let Selection {
        explicit,
        registry,
        signer,
        self_address,
    } = resolve_selection(common, chain).await?;

    // Chain plumbing for the pull loop, built once from the resolved signer.
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());
    let voucher_dom = voucher_domain(chain.chain_id, chain.payment_pool);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow!("read PaymentPool.usdc(): {e}"))?;

    // `--namespace <id>` → big-endian `uint256`; absent => `NO_NAMESPACE`
    // (best-effort cache/DHT). Same conversion as `decdn fetch`.
    let namespace_id = namespace_id(args.namespace);

    let ctx = PullCtx {
        endpoint,
        store,
        contract: &contract,
        rpc: &rpc,
        signer: &signer,
        self_address,
        token,
        voucher_dom: &voucher_dom,
        slash_dom: &slash_dom,
        chain,
        relays,
        explicit,
        registry,
        common,
        namespace_id,
        grant,
        ledgers: LaneLedgers::new(),
        funding: fetch::RunFunding::default(),
        dedup_stats: DedupStats::default(),
        open_lock: tokio::sync::Mutex::new(()),
        gate: JobGate::new(args.jobs.max(1)),
        lane_cap: LaneStreamCap::new(usize::from(args.max_lane_streams)),
        health: Arc::new(PeerHealth::default()),
        connections: Connections::new(endpoint.clone()),
        writes,
        // One clock for the whole command: a stuck entry waits while another
        // lands bytes.
        stop: StopPolicy::new(
            std::io::stderr().is_terminal(),
            common.give_up_after(),
            Arc::new(ProgressClock::new()),
        ),
        // Silent during the manifest fetch below (a single blob); replaced once
        // the kept entries are known and their sizes decide the total-bar mode.
        progress: PullProgress::disabled(),
    };

    pull_manifest(ctx, args, filter, filters_given, local_manifest).await
}

/// Print the run's wallet-shortfall warning ([`fetch::RunFunding::shortfall`]),
/// if it has one, for a run that ends before its summary.
fn warn_shortfall<P: Provider + Clone>(ctx: &PullCtx<'_, P>) {
    if let Some(shortfall) = ctx.funding.shortfall() {
        eprintln!("warning: {shortfall}");
    }
}

/// Obtain the bundle manifest through `ctx`, pull every kept entry, and print the
/// run summary.
async fn pull_manifest<P: Provider + Clone>(
    mut ctx: PullCtx<'_, P>,
    args: &BundlePullArgs,
    filter: &EntryFilter,
    filters_given: bool,
    local_manifest: Option<Kept>,
) -> anyhow::Result<()> {
    // Obtain the manifest: the pre-read local one, or the `--hash` bundle blob
    // fetched and filtered here. `None` => filtered to empty (already reported).
    // `--select`: let the user trim the (already glob-filtered) list in their
    // editor. Everything deselected ends the run (reported) like an empty filter.
    let selected = async {
        let Some(kept) = obtain_manifest(&ctx, args, filter, filters_given, local_manifest).await?
        else {
            return Ok(None);
        };
        maybe_select(args, kept)
    }
    .await;
    // The `--hash` manifest fetch is paid, so a run that ends here still owes
    // the wallet-shortfall warning the summary below prints otherwise.
    let Some(Kept { manifest, excluded }) = selected.inspect_err(|_| warn_shortfall(&ctx))? else {
        warn_shortfall(&ctx);
        return Ok(());
    };

    // The kept manifest is known: enable the multi-bar renderer (silent off a
    // terminal or under `--json`). Its total bar meters the run's **download** — the
    // bytes actually fetched after shared-chunk dedup — with the whole on-disk
    // content size shown alongside in the header, both fixed now from the manifest.
    let manifest_fetch_plan = build_fetch_plan(&manifest.entries);
    ctx.progress = PullProgress::new(
        args.json,
        download_bytes(&manifest.entries, &manifest_fetch_plan),
        total_content_bytes(&manifest.entries),
        &run_label(&args.output),
    );

    // Loaded once for the whole run: the pre-pass skip decisions above consult it,
    // and it seeds the incremental skip-cache flush inside `pull_all` — a run's
    // completed files are folded onto this prior and rewritten as the pull
    // progresses, so an interrupted pull leaves the files it landed recorded
    // rather than losing the whole index. The skip-cache is advisory, so a flush
    // failure is logged (inside the flush) and never fails the pull.
    let saved = bundle_manifest::load(&args.output);
    // Watched from here on: a Ctrl-C during the pull stops it gracefully (see
    // `pull_plain`), and the summary below still reports what landed.
    let mut interrupt = Interrupt::watch();
    let PullRun {
        outcomes,
        mut warnings,
        transfer,
        interrupted,
        stopped,
    } = ctx
        .pull_all(
            &manifest.entries,
            &args.output,
            args.overwrite,
            saved,
            &mut interrupt,
        )
        .await;
    ctx.progress.finish();
    warn_leftover_partials(&args.output, &manifest.entries, args.hash.as_deref());
    warnings.extend(ctx.funding.shortfall());

    // Every entry has joined, so the shared dedup counters are now stable.
    let dedup = DedupSummary {
        spliced_bytes: ctx.dedup_stats.spliced_bytes.load(Ordering::Relaxed),
        hints_ignored: ctx.dedup_stats.hints_ignored.load(Ordering::Relaxed),
    };
    let reported = report(
        &outcomes,
        &warnings,
        transfer,
        dedup,
        excluded,
        &args.output,
        args.json,
    );
    if interrupted {
        return Err(Interrupted.into());
    }
    // A fault that ended the whole pull is its result, so `main` maps it to
    // its exit code (75 for a give-up, 1 otherwise).
    if let Some(err) = stopped {
        return Err(err);
    }
    reported
}

/// How often the pull's poll watch ([`warn_when_unpolled`]) ticks.
const POLL_WATCH_TICK: std::time::Duration = std::time::Duration::from_secs(1);

/// How late a poll-watch tick may fire before the pull logs that its task was
/// not polled.
const POLL_WATCH_LATE: std::time::Duration = std::time::Duration::from_secs(2);

/// Run `drive` beside `flush` until both finish, or until the first Ctrl-C.
/// Returns whether a Ctrl-C stopped it.
///
/// The Ctrl-C drops `drive`: every in-flight fetch stops where it is (its drop
/// guard queues what it paid for recording, and its `.partial` is the next
/// run's resume prefix). The flush channel's sender drops with `drive`, so
/// `flush` still writes every group that settled before it returns. A poll
/// watch ([`warn_when_unpolled`]) runs beside `drive` until `drive` ends or
/// Ctrl-C stops it.
async fn drive_until_interrupted(
    drive: impl std::future::Future<Output = ()>,
    flush: impl std::future::Future<Output = ()>,
    interrupt: &mut Interrupt,
) -> bool {
    let drive = async {
        tokio::select! {
            () = drive => false,
            () = interrupt.wait() => true,
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

/// What [`PullCtx::pull_all`] hands back: every settled entry's outcome, the
/// run's byte tally, whether a Ctrl-C stopped it, and the fault that ended it
/// early. A run that stopped early leaves out the entries that had not
/// settled.
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

/// The bundle-level namespace id (ADR 002) every paid pull in the run carries:
/// `--namespace <id>` as a big-endian `uint256`, or `NO_NAMESPACE` when the flag
/// was omitted.
fn namespace_id(namespace: Option<u64>) -> [u8; 32] {
    namespace.map_or(decdn_protocol::client::NO_NAMESPACE, |n| {
        alloy::primitives::U256::from(n).to_be_bytes()
    })
}

/// The kept manifest for the run: the pre-read local one (already filtered up
/// front), or the `--hash` bundle blob fetched into memory and filtered here.
/// `Ok(None)` means the manifest is empty after filtering — [`report_nothing_to_fetch`]
/// was already called, so the caller returns `Ok(())`.
async fn obtain_manifest<P: Provider + Clone>(
    ctx: &PullCtx<'_, P>,
    args: &BundlePullArgs,
    filter: &EntryFilter,
    filters_given: bool,
    local_manifest: Option<Kept>,
) -> anyhow::Result<Option<Kept>> {
    if let Some(m) = local_manifest {
        return Ok(Some(m));
    }
    let raw = args
        .hash
        .as_deref()
        .ok_or_else(|| anyhow!("no bundle source (expected -i or --hash)"))?;
    let hash = fetch::parse_hash(raw)?;
    // "Don't pay again": a prior pull of this bundle into the same output dir
    // cached the manifest blob, keyed by its hash and self-verifying on read, so a
    // repeat run skips the paid `cdn/client/v1` fetch. `--overwrite` forces a fresh
    // fetch, consistent with how it bypasses the file skip-cache.
    let cached = (!args.overwrite)
        .then(|| bundle_cache::load(&args.output, hash))
        .flatten();
    let bytes = if let Some(cached) = cached {
        cached
    } else {
        let fetched = ctx
            .fetch_to_memory(hash, &args.output)
            .await
            .context("fetch bundle manifest blob")?;
        // Advisory: a write failure is logged, never fatal.
        bundle_cache::store(&args.output, hash, &fetched);
        fetched
    };
    let mut m = parse_manifest(&bytes)?;
    let raw_empty = m.entries.is_empty();
    let (entries, excluded) = filter.apply_and_warn(m.entries);
    m.entries = entries;
    if m.entries.is_empty() {
        report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
        warn_leftover_partials(&args.output, &[], args.hash.as_deref());
        return Ok(None);
    }
    Ok(Some(Kept {
        manifest: m,
        excluded,
    }))
}

/// Record one group's run in [`PullCtx::pull_plain`]: forward its recordable
/// files to the skip-cache flush, put its outcomes in slot `i`, and return the
/// fault that ends the whole pull, if its fetch failed with one.
fn settle_group_run(
    groups: &mut [SettledGroup],
    flush_tx: &tokio::sync::mpsc::UnboundedSender<FlushBatch>,
    i: usize,
    run: GroupRun,
    updates: BTreeMap<String, bundle_manifest::SavedFile>,
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
#[derive(Clone, Default)]
struct SettledGroup {
    outcomes: Vec<EntryOutcome>,
    /// The group's size warnings ([`size_warnings`]).
    warnings: Vec<String>,
    bytes: Option<EntryBytes>,
}

/// Run `items` through `run` once, at most `jobs` at a time. A bundle pull
/// passes every group: its [`JobGate`] is the `--jobs` cap, and a group waits
/// for its slot inside `run`.
///
/// `settle` sees every result as it lands, with the item's index in `items`,
/// and returns the fault that ends the whole run, if the result carries one.
/// That fault starts no further item, drops the items in flight, and is what
/// this returns. `None` means every item ran and settled.
async fn run_groups<G, R, Fut>(
    items: Vec<G>,
    jobs: usize,
    run: impl Fn(G) -> Fut,
    mut settle: impl FnMut(usize, R) -> Option<anyhow::Error>,
) -> Option<anyhow::Error>
where
    Fut: std::future::Future<Output = R>,
{
    let width = jobs.min(items.len()).max(1);
    let mut stream = futures_util::stream::iter(items.into_iter().enumerate())
        .map(|(i, item)| {
            let fut = run(item);
            async move { (i, fut.await) }
        })
        .buffer_unordered(width);
    while let Some((i, result)) = stream.next().await {
        if let Some(err) = settle(i, result) {
            return Some(err);
        }
    }
    None
}

/// Per-provider cap on concurrent streams to one `(pool, signer, provider)`
/// lane. A lane has one shared [`LaneLedgers`] voucher watermark; concurrent
/// streams on the same lane draw on it together, and the serving node credits
/// each stream's delivered bytes from that shared watermark fairly — a slower
/// stream, whose reveal for its own chunk can land below a faster sibling's
/// frontier, is paid from the lane headroom the sibling opened rather than
/// stalled. So same-lane concurrency is safe; this only bounds how many streams
/// touch a given provider at once. `--max-lane-streams` sets the cap (default 4):
/// a `Semaphore(N)` admits N concurrent same-lane streams, and N == 1 runs a
/// single ordered voucher sequence per lane. Cross-lane parallelism (distinct
/// providers) is never bounded here — only by `--jobs`.
pub(crate) struct LaneStreamCap {
    /// Per-provider streams, created on first use. The `tokio::sync::Mutex`
    /// guards the map so the cap is `Sync` and shareable across the entry futures.
    map: tokio::sync::Mutex<HashMap<Address, Arc<ProviderStreams>>>,
    /// Concurrent-stream permits per provider (at least 1). At 1 a `Semaphore(1)`
    /// serializes same-lane streams exactly like a mutex.
    n: usize,
    /// The id the next lane's [`ExtraPermits`] gets.
    next_lane: AtomicU64,
}

impl LaneStreamCap {
    /// A cap admitting `n` concurrent streams per provider (clamped to at least 1).
    pub(crate) fn new(n: usize) -> Self {
        Self {
            map: tokio::sync::Mutex::new(HashMap::new()),
            n: n.max(1),
            next_lane: AtomicU64::new(0),
        }
    }

    /// The per-provider streams, created on first use.
    async fn streams(&self, provider: Address) -> Arc<ProviderStreams> {
        let mut map = self.map.lock().await;
        Arc::clone(
            map.entry(provider)
                .or_insert_with(|| Arc::new(ProviderStreams::new(self.n))),
        )
    }

    /// One stream permit for `provider` if one is free now, else `None`. It
    /// never waits, so a caller that already holds another provider's permit
    /// cannot deadlock against a sibling that holds this one. It does not
    /// look at the provider's claims ([`ExtraPermits::grant`]): a lane's first
    /// stream takes any free permit, and a lane that finds none backs off and
    /// holds nothing.
    pub(crate) async fn try_permit(
        &self,
        provider: Address,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.streams(provider).await.semaphore)
            .try_acquire_owned()
            .ok()
    }

    /// How a lane to `provider` takes a stream beyond its lease
    /// ([`decdn_client::LaneWiden`]): an extra stream for a queued range no
    /// idle lane takes, or the stream it starts again on. `grow` takes only
    /// a permit of `provider` that is free now and never waits, and `release`
    /// gives one back. The lane's first stream holds its permit as its lease,
    /// so the extra streams fit in the rest of the cap. At a cap of 3 or more,
    /// an extra stream never takes the provider's last free permit, so a
    /// sibling entry's first stream to `provider` can still open; the extra
    /// stream holds its permit for a whole range. A smaller cap has no room
    /// for both, and an extra stream takes any free permit. The acquire gives
    /// the lease back when the lane's own worker ends, and the lane starts
    /// again only on a permit `grow` takes; that start may take the last
    /// permit that no claim holds back.
    ///
    /// A lane that `grow` refuses claims the provider's next free permit,
    /// ahead of every later `grow` ask, and a lane that runs no stream goes
    /// ahead of the claims of lanes that run one ([`ProviderStreams::take`],
    /// #2341). A lane's first stream ([`Self::try_permit`]) still takes any
    /// free permit.
    pub(crate) async fn widen(&self, provider: Address) -> decdn_client::LaneWiden {
        let extras = Arc::new(self.extra_permits(provider).await);
        let released = Arc::clone(&extras);
        decdn_client::LaneWiden::new(
            move |kind| extras.grant(kind),
            move || released.release_one(),
        )
    }

    /// A new lane's grants of `provider`'s permits beside its lease, under a
    /// lane id no other lane of this cap has.
    async fn extra_permits(&self, provider: Address) -> ExtraPermits {
        ExtraPermits {
            streams: self.streams(provider).await,
            keep: self.extra_keep(),
            lane: self.next_lane.fetch_add(1, Ordering::Relaxed),
            held: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// The free permits of a provider an extra stream leaves for a sibling
    /// entry's first stream: one at a cap of 3 or more, where the lease, an
    /// extra stream and the kept permit fit, and none below.
    const fn extra_keep(&self) -> usize {
        if self.n > 2 { 1 } else { 0 }
    }
}

/// How long a lane's claim on a provider's next free permit holds without a
/// new ask. The acquire asks a refused `grow` again within
/// [`decdn_client::GROWTH_RETRY`] while it still wants the stream, and each
/// ask renews the claim. So a lane that still waits keeps its claim, and the
/// claim of a lane that stopped asking, such as one whose own worker took
/// the range, ends soon after. A lane whose node refused its extra streams
/// is not asked while it waits out that refusal, so its claim lapses too:
/// the node would refuse the stream anyway.
const CLAIM_TTL: std::time::Duration = decdn_client::GROWTH_RETRY.saturating_mul(3);

/// One provider's stream permits and the claims of the lanes that wait for
/// one.
struct ProviderStreams {
    /// The provider's per-lane stream semaphore.
    semaphore: Arc<tokio::sync::Semaphore>,
    /// The lanes [`Self::take`] refused, at most one claim per lane, in the
    /// order they first claimed. A renewal keeps a lane's place.
    claims: std::sync::Mutex<Vec<Claim>>,
}

/// A lane's claim on its provider's next free permit.
struct Claim {
    /// The claiming lane's [`ExtraPermits::lane`].
    lane: u64,
    /// The free permits the lane's last ask leaves after its own.
    keep: usize,
    /// Whether the lane ran no stream at its last ask.
    bare: bool,
    /// When the lane last asked.
    at: tokio::time::Instant,
}

impl ProviderStreams {
    /// `n` permits and no claims.
    fn new(n: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(n)),
            claims: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// A permit for `lane` if one is free now that no claim holds back,
    /// never waiting. `keep` is the free permits the ask leaves after its
    /// own, and `bare` says the lane runs no stream.
    ///
    /// Each claim ahead of `lane`'s own, or every claim when it has none,
    /// holds back one free permit, and the largest `keep` among the ask and
    /// those claims stays free after them. A bare ask counts only the bare
    /// claims ahead, so a lane that runs no stream goes ahead of a lane that
    /// waits for one more: every lane gets its first stream before any lane
    /// gets a second. So a lane that asks the instant a permit frees, such
    /// as one whose short legs restart beside its extra streams, cannot take
    /// it from a lane that asks only once per growth pass (#2341).
    ///
    /// A refused ask claims the next free permit, or renews the lane's claim
    /// in its place. A grant ends the claim. Claims older than [`CLAIM_TTL`]
    /// do not count. Every entry is polled on one task and this never
    /// awaits, so no sibling takes a permit between the count and the take.
    fn take(
        &self,
        lane: u64,
        keep: usize,
        bare: bool,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let now = tokio::time::Instant::now();
        let mut claims = self.claims.lock().unwrap_or_else(PoisonError::into_inner);
        claims.retain(|claim| now.saturating_duration_since(claim.at) < CLAIM_TTL);
        let own = claims.iter().position(|claim| claim.lane == lane);
        let (ahead, kept) = claims
            .get(..own.unwrap_or(claims.len()))
            .unwrap_or_default()
            .iter()
            .filter(|claim| !bare || claim.bare)
            .fold((0_usize, keep), |(n, kept), claim| {
                (n.saturating_add(1), kept.max(claim.keep))
            });
        if self.semaphore.available_permits() <= ahead.saturating_add(kept) {
            let claim = Claim {
                lane,
                keep,
                bare,
                at: now,
            };
            match own.and_then(|i| claims.get_mut(i)) {
                Some(renewed) => *renewed = claim,
                None => claims.push(claim),
            }
            return None;
        }
        match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => {
                if let Some(i) = own {
                    claims.remove(i);
                }
                Some(permit)
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => None,
            Err(tokio::sync::TryAcquireError::Closed) => {
                // Nothing closes the semaphore: a closed one grants no
                // stream, and says so.
                tracing::warn!("a lane's stream semaphore is closed; it grants no extra stream");
                None
            }
        }
    }

    /// End `lane`'s claim, if it has one.
    fn forget(&self, lane: u64) {
        self.claims
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|claim| claim.lane != lane);
    }
}

/// The stream permits one lane holds beside its lease: its extra streams',
/// and its own stream's once it starts again without the lease
/// ([`LaneStreamCap::widen`]).
struct ExtraPermits {
    /// The provider's permits and claims.
    streams: Arc<ProviderStreams>,
    /// The free permits an extra stream leaves for a sibling entry's first
    /// stream.
    keep: usize,
    /// The lane's id in its provider's claims.
    lane: u64,
    /// The permits granted and not yet given back.
    held: std::sync::Mutex<Vec<tokio::sync::OwnedSemaphorePermit>>,
}

impl ExtraPermits {
    /// Take one permit if it is free now and no claim holds it back
    /// ([`ProviderStreams::take`]), never waiting, and return whether one
    /// was taken. An extra stream leaves `keep` free permits for a sibling
    /// entry's first stream; a restart may take the last one. The acquire
    /// asks a restart only once the lane's lease is given back, so a lane
    /// that asks one and holds no grant runs no stream.
    fn grant(&self, kind: decdn_client::GrowFor) -> bool {
        let (keep, bare) = match kind {
            decdn_client::GrowFor::Restart => (0, self.lock_held().is_empty()),
            decdn_client::GrowFor::Extra => (self.keep, false),
        };
        let Some(permit) = self.streams.take(self.lane, keep, bare) else {
            return false;
        };
        self.lock_held().push(permit);
        true
    }

    /// Give back one held permit. The acquire gives back only what `grant`
    /// granted, so a call with none held breaks that rule: it does nothing
    /// and says so.
    fn release_one(&self) {
        let permit = self.lock_held().pop();
        debug_assert!(
            permit.is_some(),
            "a stream was given back that was never granted"
        );
        if permit.is_none() {
            tracing::warn!("a lane gave back a stream permit it does not hold");
        }
        drop(permit);
    }

    /// The held permits, locked.
    fn lock_held(&self) -> std::sync::MutexGuard<'_, Vec<tokio::sync::OwnedSemaphorePermit>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for ExtraPermits {
    /// An ended lane claims nothing.
    fn drop(&mut self) {
        self.streams.forget(self.lane);
    }
}

/// The run's `--jobs` cap ([`PullCtx::gate`]): its permits, and the admission
/// queue a new group's first permit waits in.
///
/// A new group waits in `admission` before it waits for a permit, so at most
/// one new group queues on `permits` at a time. A group that gave its permit
/// back while it waited for donors ([`FetchSlot::ensure`]) queues on `permits`
/// directly, so it waits behind at most that one new group, not behind every
/// group of the bundle still to start.
struct JobGate {
    permits: tokio::sync::Semaphore,
    admission: tokio::sync::Mutex<()>,
}

impl JobGate {
    /// A gate of `jobs` permits.
    fn new(jobs: usize) -> Self {
        Self {
            permits: tokio::sync::Semaphore::new(jobs),
            admission: tokio::sync::Mutex::new(()),
        }
    }

    /// Wait for a permit.
    async fn permit(&self) -> anyhow::Result<tokio::sync::SemaphorePermit<'_>> {
        self.permits
            .acquire()
            .await
            .map_err(|_| anyhow!("bundle pull concurrency gate closed"))
    }
}

/// One `--jobs` permit of a [`JobGate`], held by one hash group's fetch or by
/// the manifest fetch.
///
/// A range-dedup entry gives it back only while it waits for a sibling to
/// register its donor chunks ([`RangeDriver::release_slot`]): a waiter moves
/// no bytes, and holding the permit would keep a queued entry from fetching.
/// The wait takes a permit again as it ends ([`RangeDriver::retake_slot`]), so
/// the splice, the whole-file hash and the materialize after it run under one,
/// and each drive takes one again first ([`Self::ensure`]). One entry uses a
/// slot sequentially: it never drives twice at once.
struct FetchSlot<'g> {
    gate: &'g JobGate,
    permit: Mutex<Option<tokio::sync::SemaphorePermit<'g>>>,
}

impl<'g> FetchSlot<'g> {
    /// Wait in `gate`'s admission queue, then for a permit.
    async fn acquire(gate: &'g JobGate) -> anyhow::Result<Self> {
        let admitted = gate.admission.lock().await;
        let permit = gate.permit().await?;
        drop(admitted);
        Ok(Self {
            gate,
            permit: Mutex::new(Some(permit)),
        })
    }

    /// Give the permit back, if this slot holds it.
    fn release(&self) {
        drop(
            self.permit
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
    }

    /// Hold a permit: wait for one if [`Self::release`] gave it back, behind
    /// at most the one new group in admission. A permit that arrives after
    /// another already filled the slot goes straight back.
    async fn ensure(&self) -> anyhow::Result<()> {
        if self.held() {
            return Ok(());
        }
        let permit = self.gate.permit().await?;
        let mut slot = self.permit.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(permit);
        }
        Ok(())
    }

    /// Whether this slot holds a permit now.
    fn held(&self) -> bool {
        self.permit
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }
}

/// Shared, by-reference state for the entry fetch loop. Borrowed by every
/// in-flight entry future, and every entry must be polled on one task, as
/// `buffer_unordered` does: each entry reads its lanes' ledgers and queues
/// their watermark writes with no await between
/// ([`fetch::queue_face_watermarks`]), and only one task keeps another entry's
/// read from falling between them.
struct PullCtx<'a, P: Provider + Clone> {
    endpoint: &'a Endpoint,
    store: &'a Arc<RedbBuyerPoolStore>,
    contract: &'a PaymentPool::PaymentPoolInstance<P>,
    rpc: &'a P,
    signer: &'a Arc<PrivateKeySigner>,
    self_address: Address,
    /// The pool's settlement token (USDC), read once via `PaymentPool.usdc()`.
    token: Address,
    voucher_dom: &'a Eip712Domain,
    slash_dom: &'a Eip712Domain,
    chain: &'a fetch::ResolvedChain,
    relays: &'a [RelayUrl],
    /// `Some((node_id, provider))` pins every entry to one node (`--node-id`);
    /// `None` discovers per entry against a sample of `registry`.
    explicit: Option<FetchTarget>,
    /// Every active registered node, read once for the run; `None` when
    /// `explicit` pins the node.
    registry: Option<Vec<NodeCandidate>>,
    common: &'a ClientFetchArgs,
    /// Delegated capability adopted for every entry (`--capability`), or `None`
    /// for the self-owned pool path. When `Some`, every entry presents this
    /// owner-signed grant instead of opening/reusing the caller's own pool, and
    /// reactive top-up is disabled (the delegate owns no pool to fund).
    grant: Option<decdn_incentive::CapabilityGrant>,
    /// Bundle-level namespace id (ADR 002) applied to every paid pull in the run —
    /// `--namespace <id>` as a big-endian `uint256`; `NO_NAMESPACE` when the flag
    /// was omitted.
    namespace_id: [u8; 32],
    /// The run's shared per-lane voucher ledgers (ADR 039): every entry and
    /// chunk on a `(pool_id, signer, provider)` lane draws on one monotonic
    /// issuer, so concurrent same-lane fetches never race the cumulative
    /// watermark.
    ledgers: LaneLedgers,
    /// The run's funding facts ([`fetch::RunFunding`]): the pool's spend
    /// outside the run's lanes, and a wallet too short of USDC to top it up.
    funding: fetch::RunFunding,
    /// Run-scoped range-dedup counters (bytes spliced from disk, hints dropped by
    /// a fault), accumulated by every entry and reported once the pull finishes.
    dedup_stats: DedupStats,
    /// Serializes every pool open-or-reuse across the whole bundle. The
    /// bundle's every entry shares ONE `PaymentPool` deposit (ADR 003), so
    /// distinct providers cannot open concurrently: its on-chain state (deposit,
    /// standing USDC allowance) is one resource regardless of which provider an
    /// entry is bound for. Taken around the whole open-or-reuse call (which may
    /// also perform a low-water top-up) and released before streaming, so
    /// delivery itself still runs concurrently once each entry has its context.
    open_lock: tokio::sync::Mutex<()>,
    /// Global `--jobs` cap (min 1) on hash groups at work, one permit per group
    /// ([`FetchSlot`]), taken before the group's file bar: a plain whole-file
    /// fetch, or a hint-carrying entry's complement drive, donor splice and any
    /// re-fetch. A range-dedup entry gives its permit back while it only waits
    /// for a sibling's donor chunks, and takes one again to drive. A byte range
    /// a sibling entry already holds is spliced from disk instead of fetched,
    /// so it never takes a permit of its own.
    gate: JobGate,
    /// Per-provider cap on concurrent same-lane streams (`--max-lane-streams`,
    /// default 4). Every lane an entry's fetch builds holds one of its
    /// provider's permits, taken without waiting ([`CliSources`]). Bounds only
    /// per-provider concurrency; `--jobs` still bounds cross-lane parallelism.
    lane_cap: LaneStreamCap,
    /// The command-wide holder health every entry's fetch records into, so a
    /// holder that faults one entry cools for every entry.
    health: Arc<PeerHealth>,
    /// The command's one connection per node: every entry's lanes, the
    /// range-dedup entries' included, open their streams on it.
    connections: Connections,
    /// The command's blocking state writes, in queue order across every
    /// entry ([`OrderedWrites`]).
    writes: &'a OrderedWrites,
    /// The command's stop policy. Its progress clock is shared by every entry
    /// and the manifest fetch, so the pull gives up only once no entry has
    /// landed a verified byte for the whole limit.
    stop: StopPolicy,
    /// The run's multi-bar progress renderer: one per-file bar per active pull
    /// above a bottom total bar (silent off a terminal or under `--json`). Set
    /// once the kept manifest is known — its entries decide the total-bar mode —
    /// so it starts [disabled](PullProgress::disabled) during the manifest fetch.
    progress: PullProgress,
}

impl<P: Provider + Clone> PullCtx<'_, P> {
    /// The shared lane deps, assembled from `PullCtx`'s borrowed chain
    /// plumbing plus this run's per-fetch budgets — the same shape `decdn fetch`
    /// builds, for every fetch of an entry ([`Self::acquire_entry`]).
    fn drive_deps(&self, max_blob_bytes: u64) -> anyhow::Result<fetch::DriveFetchDeps<'_, P>> {
        Ok(fetch::DriveFetchDeps {
            timings: None,
            endpoint: self.endpoint,
            store: self.store,
            contract: self.contract,
            rpc: self.rpc,
            slash_dom: self.slash_dom,
            self_address: self.self_address,
            token: self.token,
            chain: self.chain,
            // Bundle-level `--namespace` (ADR 002): routes any cache-miss origin
            // pull to that namespace's authorized origins. Applies uniformly to the
            // manifest blob and every entry — all funnel through here.
            namespace_id: self.namespace_id,
            max_rate_per_mb: self.common.max_rate_per_mb,
            max_blob_bytes,
            // Same shape as `fetch` (#1134): a node that accepts the connection and
            // never answers is as dead as one that stops mid-stream, so the same
            // budget bounds both stages. The pull carries no overall wall-clock cap —
            // `drive` never consults one — so it completes for any blob size as long
            // as the upstream keeps feeding it bytes.
            deadlines: PullDeadlines::new(fetch::STALL_WINDOW, fetch::STALL_WINDOW, 0)?,
            connections: &self.connections,
            writes: self.writes,
            funding: &self.funding,
        })
    }

    /// Fetch one whole blob into `staging` through the acquire loop (ADR 039),
    /// across the entry's holders ([`Self::entry_targets`]). See
    /// [`Self::acquire_entry`] for the fetch itself and what it returns.
    ///
    /// `slot` is the caller's `--jobs` permit; the fetch runs under it.
    async fn fetch_to_staging(
        &self,
        hash: [u8; 32],
        staging: &Path,
        total: Option<u64>,
        progress: Option<&ProgressCallback>,
        slot: &FetchSlot<'_>,
    ) -> anyhow::Result<bool> {
        slot.ensure().await?;
        let targets = self.entry_targets(hash).await?;
        self.acquire_entry(&targets, hash, staging, total, None, progress)
            .await
    }

    /// One entry's holders: the pinned `--node-id`, or a fresh probe of the
    /// entry's own registry sample (proxy-warming non-holders first when they
    /// help, then holders nearest RTT first). An entry resolves them once and
    /// every fetch of the entry reuses them. A resolution that fails for any
    /// reason but the configuration ([`fetch::holders_or_none`]) starts the
    /// entry with no holder, and its acquire loop discovers them.
    async fn entry_targets(&self, hash: [u8; 32]) -> anyhow::Result<fetch::ResolvedTargets> {
        fetch::holders_or_none(self.resolve_entry_targets(hash).await)
    }

    /// [`Self::entry_targets`] before a failure is sorted into a configuration
    /// fault or an empty start.
    async fn resolve_entry_targets(
        &self,
        hash: [u8; 32],
    ) -> anyhow::Result<fetch::ResolvedTargets> {
        if self.explicit.is_some() {
            return fetch::resolve_target_node(
                self.common,
                self.chain,
                self.endpoint,
                self.relays,
                hash,
                fetch::ProbeOpts {
                    timings: None,
                    round: fetch::ProbeRound::Stream,
                },
            )
            .await;
        }
        // The entry's round streams: its fetch starts at the first verified
        // holder, and the holders that answer later join it.
        let candidates = self.entry_candidates()?;
        fetch::probe_and_order(
            self.endpoint,
            &candidates,
            self.relays.first(),
            hash,
            fetch::ProxyWarmingParams::from_args(self.common),
            self.slash_dom,
            fetch::ProbeRound::Stream,
        )
        .await
    }

    /// Fill `ranges` of blob `hash` (`None` is the whole blob) into the ranged
    /// store beside `staging` through the acquire loop, across `targets`' holders.
    /// The store is promoted to `staging` once every byte is present.
    ///
    /// `total` is the entry's first size claim: the manifest's `size` when it
    /// gives one. The fetch grows or shrinks it as bytes land, so a manifest
    /// size that differs from the blob never fails the entry. With no manifest
    /// size (the manifest blob itself, or an unsized entry), the first claim
    /// comes from the probe's size hint, or else from a header-only first open.
    ///
    /// A holder that faults cools in the command-wide [`PeerHealth`] and
    /// returns inside the same loop, so a holder that fails one entry is
    /// rested for every entry. The fetch ends on done, a fault only the user
    /// can fix, a unanimous verdict of the holders, or the command's
    /// [`StopPolicy`]. Every lane draws vouchers from the run's shared
    /// `LaneLedgers` (ADR 039), serializes its pool open-or-reuse on
    /// `open_lock`, and holds one of its provider's stream permits
    /// ([`LaneStreamCap`]). Each lane's voucher watermark is persisted before
    /// the result returns, and the `.partial` beside `staging` stays for a
    /// resume.
    ///
    /// It returns `true` when the store's finalized file failed its hash and
    /// the fetch dropped every byte the store held and fetched the blob again
    /// ([`Downloader::refetched_targets`]). A prefix an earlier run left is
    /// then fetched and paid again.
    async fn acquire_entry(
        &self,
        targets: &fetch::ResolvedTargets,
        hash: [u8; 32],
        staging: &Path,
        total: Option<u64>,
        ranges: Option<&[(u64, u64)]>,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<bool> {
        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let deps = self.drive_deps(max_blob_bytes)?;
        let sources = CliSources::new(
            &deps,
            self.common,
            self.relays,
            self.grant.as_ref(),
            self.signer,
            self.voucher_dom,
            Some(&self.open_lock),
            Some(&self.ledgers),
            Some(&self.lane_cap),
        );
        let holders = sources.holders_from(targets);
        // A fetch dropped by Ctrl-C, or by a sibling's command-wide fault,
        // queues every lane's vouchers for recording too.
        let on_drop = fetch::SettleOnDrop::new(|| sources.persist_watermarks_detached());
        let result = async {
            // The manifest's size is the entry's first claim; without one, the
            // probe's hint or a header-only open gives it.
            let (total_bytes, holders) = if let Some(total) = total {
                (total, holders)
            } else {
                let claim = sources
                    .first_claim(hash, holders, &self.health, &self.stop)
                    .await?;
                (claim.total_bytes, claim.holders)
            };
            // `--max-sources` caps the lanes the entry stripes across at
            // once; every other holder waits as a reserve.
            let downloader = Downloader::new(&sources, sources.funder())
                .holders(holders)
                .health(Arc::clone(&self.health))
                .working_deposit(self.chain.working_deposit)
                .max_lanes(self.common.max_sources)
                .max_blob_bytes(max_blob_bytes);
            Box::pin(downloader.fetch_to_paths_shared(
                &[DownloadTarget {
                    hash,
                    total_bytes,
                    dest: staging,
                    ranges,
                }],
                &self.ledgers,
                progress,
                &self.stop,
            ))
            .await
            .map(|_paths| downloader.refetched_targets() > 0)
        }
        .await;
        on_drop.disarm();
        sources.persist_watermarks().await;
        result.map_err(|err| sources.annotate(err))
    }

    /// This entry's probe candidates: a fresh sample of the run's registry read
    /// ([`entry_candidates`]).
    fn entry_candidates(&self) -> anyhow::Result<Vec<NodeCandidate>> {
        let registry = self
            .registry
            .as_deref()
            .ok_or_else(|| anyhow!("no discovery candidates available"))?;
        Ok(entry_candidates(registry, self.chain.region.as_deref()))
    }

    /// Fetch one blob fully into memory — used only for the bundle manifest
    /// itself when named by `--hash` rather than read locally via `-i`.
    /// Manifests are small (unlike bundle entries, which stream straight to
    /// their destination and never buffer the whole blob), so streaming into a
    /// staging file under `out_root` and reading it back is cheap; it also
    /// means a manifest fetch runs the same acquire loop, under the same
    /// health table and progress clock, as every entry. A manifest has no
    /// manifest size, so the fetch takes the first size claim (the probe hint,
    /// or a header-only open when there is none). The staging
    /// file is removed once read back — a manifest fetch has nothing further
    /// to resume once its bytes are safely in memory.
    async fn fetch_to_memory(&self, hash: [u8; 32], out_root: &Path) -> anyhow::Result<Vec<u8>> {
        let staging = staging_path(out_root, hash)?;
        // The manifest blob fetch is silent (no bar): `progress` is disabled here
        // anyway, and the per-file bars belong to the entries, not the manifest.
        let slot = FetchSlot::acquire(&self.gate).await?;
        self.fetch_to_staging(hash, &staging, None, None, &slot)
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
    /// one `--jobs` cap; a group's file bar appears only once it holds a slot.
    /// Parallelism comes from concurrent in-flight network I/O, while the run's
    /// shared `LaneLedgers` keep same-lane voucher issuance monotonic across
    /// every entry.
    ///
    /// A run-wide [`ChunkIndex`] threads through every group: an entry that
    /// carries chunk hints and completes registers its chunks, and a later entry
    /// that shares a chunk splices those already-materialized byte ranges from
    /// disk instead of paying to fetch them (range-dedup). A donor's finalized
    /// staging blob is therefore kept until the whole run finishes, then swept.
    async fn pull_all(
        &self,
        entries: &[ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        saved: SavedManifest,
        interrupt: &mut Interrupt,
    ) -> PullRun {
        // Every entry declares an authoritative whole-file `hash`, so the by-hash
        // grouping path (fetch-once + link-duplicates, #1306) covers plain and
        // hint-carrying entries alike — the chunk hints only change HOW a group's
        // one blob is assembled, never that it is one paid unit per distinct hash.
        let index = ChunkIndex::default();
        let refs: Vec<&ManifestEntry> = entries.iter().collect();
        // Assign every chunk shared by two or more entries to its smallest holder,
        // so that shared chunk is fetched once (by that holder) and spliced into
        // every other entry — computed once from the whole manifest.
        let fetch_plan = build_fetch_plan(entries);
        // Pre-pass: classify every entry against the output tree and the saved
        // skip-cache before any fetch, so an updated file at an already-present
        // path is written rather than silently skipped. The re-hash is this
        // command's own work, so the progress clock holds while it runs, and
        // the time since the manifest landed (an editor under `--select`
        // included) ends when it does.
        let hold = self.stop.clock.hold();
        let disk = resolve_disk_state(entries, &saved, out_root, overwrite).await;
        drop(hold);
        for d in &disk.seed {
            index.seed_disk(d.hash, &d.source, d.offset, d.len);
        }
        // `saved` moves on to seed the incremental skip-cache flush: each
        // completed group's records fold onto it and rewrite the cache as the
        // pull progresses.
        let run = self
            .pull_plain(
                &refs,
                out_root,
                overwrite,
                &index,
                &fetch_plan,
                &disk,
                saved,
                interrupt,
            )
            .await;

        // A donor entry's finalized staging blob is the source a recipient splices
        // from, so it is kept past its own group's cleanup. With the run over,
        // every registered donor source is safe to remove — EXCEPT one a
        // `mark_retained` flagged: its blob is paid for but a destination failed to
        // materialize, so the finalized `<hex>` is the resume prefix a rerun needs
        // (deleting it would force a full re-fetch and re-payment).
        //
        // Disk cost: a donor's first destination is a hard link to its staging
        // blob, so the two share storage until this sweep drops the staging name
        // (a copy only when the destination is on another filesystem).
        sweep_donor_sources(&index).await;
        run
    }

    /// Fetch every distinct blob once (grouped by hash) and materialize it at each
    /// destination path (#1306), routing each group through [`Self::pull_entry`] so a
    /// blob whose chunk hints overlap an already-materialized sibling pays only for
    /// the complement. `PullCtx.gate` caps the groups at work at `--jobs`; a
    /// range-dedup entry waiting for donors holds no slot, so a queued group
    /// starts in its place.
    // The run-wide context (index, fetch plan, disk pre-pass, flush cache) all
    // threads through here; bundling it into a struct would only rename the fields.
    #[allow(clippy::too_many_arguments)]
    async fn pull_plain<'e>(
        &self,
        entries: &[&'e ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        index: &ChunkIndex,
        fetch_plan: &FetchPlan,
        disk: &DiskState,
        saved: SavedManifest,
        interrupt: &mut Interrupt,
    ) -> PullRun {
        let groups_by_hash = order_groups_smallest_first(group_by_hash(entries));
        let group_count = groups_by_hash.len().max(1);
        // Completed groups' skip-cache records flow to the flush task over this
        // channel. Unbounded so a `send` from the fetch-driving side never blocks
        // (the flush write must never stall a fetch); the messages are one small
        // batch per completed group, so the channel stays shallow.
        let (flush_tx, flush_rx) = tokio::sync::mpsc::unbounded_channel::<FlushBatch>();

        // The fetch-driving side: run every group through `fetch_group` once,
        // every one in flight and each waiting there for its `--jobs` slot. As
        // each run completes, forward its recordable files (built here while
        // the group's entries are in scope) to the flush task and keep its
        // outcomes for the run summary. A fault that ends the whole pull
        // ([`ends_the_pull`]) drops every group still in flight, so none that
        // waits for a slot starts.
        let mut groups: Vec<SettledGroup> = vec![SettledGroup::default(); groups_by_hash.len()];
        let mut stopped: Option<anyhow::Error> = None;
        let drive = async {
            let run = |group: HashGroup<'e>| {
                // Cheap clone of the group's entry refs so `fetch_group` can consume
                // `group` while we still correlate outcomes to entries.
                let group_entries: Vec<&'e ManifestEntry> = group.entries.clone();
                async move {
                    let run = self
                        .fetch_group(group, out_root, overwrite, index, fetch_plan, disk)
                        .await;
                    // Mark this group's blob finished (whatever the outcome) so a
                    // tail-reconcile waiter deferring a chunk assigned to it stops
                    // waiting and pays for the range itself if the group failed to
                    // register that chunk.
                    if let Some(en) = group_entries.first()
                        && let Ok(whole) = fetch::parse_hash(&en.hash)
                    {
                        index.mark_finished(whole);
                    }
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
                    let updates = build_completed_updates(&group_entries, &run.outcomes, out_root);
                    let warnings = size_warnings(&group_entries, &updates);
                    (run, updates, warnings)
                }
            };
            stopped = run_groups(
                groups_by_hash,
                // Every group in flight at once: each waits for its slot of the
                // global gate, the one `--jobs` cap, in start order.
                group_count,
                run,
                |i, (run, updates, warnings)| {
                    settle_group_run(&mut groups, &flush_tx, i, run, updates, warnings)
                },
            )
            .await;
            // Closing the channel tells the flush task to do its final write.
            drop(flush_tx);
        };
        // The flush task: fold each batch onto the prior skip-cache and rewrite it
        // atomically at the cadence, plus a final write when the channel closes.
        // Running here (joined, not awaited inside `drive`) keeps the blocking
        // write off the fetch-driving path — `join!` keeps polling `drive` while
        // this side awaits its write.
        let flush = flush_task(out_root, saved, flush_rx);

        let interrupted = drive_until_interrupted(drive, flush, interrupt).await;
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

    /// Run [`Self::pull_entry_untimed`] and, when the entry lands, log one `-v`
    /// line with its size, the time the whole entry took, and its download rate
    /// (#2120, #2189). Returns the entry's [`EntryBytes`] (see
    /// [`Self::pull_entry_untimed`]).
    #[allow(clippy::too_many_arguments)]
    async fn pull_entry(
        &self,
        hash: [u8; 32],
        hints: Option<&[Hint]>,
        total: Option<u64>,
        staging: &Path,
        index: &ChunkIndex,
        fetch_plan: &FetchPlan,
        file: Option<&pull_progress::FileBar>,
        slot: &FetchSlot<'_>,
    ) -> anyhow::Result<EntryBytes> {
        let started = std::time::Instant::now();
        self.pull_entry_untimed(hash, hints, total, staging, index, fetch_plan, file, slot)
            .await
            .inspect(|&bytes| log_entry_done(hash, staging, bytes, started.elapsed()))
    }

    /// Reconstruct one entry's blob into `staging` (the finalized per-hash staging
    /// file [`Self::fetch_group`] then materializes to each destination), using chunk
    /// hints to dedup byte ranges against the run's [`ChunkIndex`] when they help.
    ///
    /// - No hints (or `total` unknown, or the blob is already finalized at
    ///   `staging`), or no chunk overlaps a materialized sibling → the plain
    ///   whole-file [`Self::fetch_to_staging`] path.
    /// - Otherwise the dedup path: pay through an [`AcquireRangeDriver`] for only
    ///   the *complement* (the group-aligned bytes no donor covers) into `staging`'s
    ///   `.partial`, then splice each donor range from its sibling's on-disk blob.
    ///   Each donor chunk is confirmed present at the recorded source offset by
    ///   re-hashing the whole chunk against its hint hash before its bytes are
    ///   trusted; a chunk that fails (a lying donor hint, a short read) is added to
    ///   a re-fetch list and driven normally. The reassembled `.partial` is then
    ///   verified whole against the authoritative `hash`; on a match it is promoted
    ///   to `staging`, and on a mismatch (a lying *recipient* hint mis-placed a
    ///   chunk) the whole blob is re-driven and re-verified before the entry fails.
    ///
    /// On success the entry's chunks are registered into `index` so later entries
    /// can splice from this blob, and its [`EntryBytes`] are returned. Before
    /// either path drives anything, the entry reads the bytes its ranged store
    /// already holds from an earlier, interrupted run ([`resumed_spans`]); those
    /// are resumed, not paid. The paid count is the whole blob less its resumed
    /// bytes on the plain path, the blob less its spliced and resumed bytes on the
    /// dedup path, and 0 for an already-finalized staging blob. It is a content
    /// count: bao proof overhead is not in it.
    #[allow(clippy::too_many_arguments)]
    async fn pull_entry_untimed(
        &self,
        hash: [u8; 32],
        hints: Option<&[Hint]>,
        total: Option<u64>,
        staging: &Path,
        index: &ChunkIndex,
        fetch_plan: &FetchPlan,
        file: Option<&pull_progress::FileBar>,
        slot: &FetchSlot<'_>,
    ) -> anyhow::Result<EntryBytes> {
        // The byte-delivery callback drives the file bar's download and the total
        // bar's download meter; the `file` handle also carries the phase transitions
        // (discovering / pending / reconstructing) the byte callback cannot express.
        let progress = file.and_then(|f| f.callback());
        // An already-finalized `<hex>` staging blob — a prior run promoted it, or
        // this run finalized it and crashed in the promote-to-materialize window —
        // is the complete blob, BLAKE3-verified when it was promoted. Materialize
        // straight from it: `fetch_group` copies `staging` to each destination, so
        // re-hash it once as a cheap guard and return. It must NEVER be re-driven:
        // the ranged store keys resume on the `.ranges` sidecar, which promotion
        // deletes, so `open_or_create` on a sidecar-less `<hex>` would `create`
        // (truncate) it and re-pay for the whole blob. On a mismatch (a corrupt
        // leftover) drop it and fall through to a normal fetch.
        if staging.try_exists().unwrap_or(false) {
            let staging_buf = staging.to_path_buf();
            let verified = tokio::task::spawn_blocking(move || {
                hash_partial(&staging_buf).is_ok_and(|got| got == hash)
            })
            .await
            .map_err(|e| anyhow!("staging verify task: {e}"))?;
            if verified {
                if let (Some(cb), Some(total)) = (progress, total) {
                    cb(total, total);
                }
                index.register(hints, staging);
                return Ok(EntryBytes::default());
            }
            remove_staging_off_runtime(staging).await;
        }

        // What earlier runs already fetched into this entry's `.partial`: read now,
        // before any drive of this run adds to it. `staging` is absent here, so
        // the read opens the record alone and never hashes a final file.
        let resumed = resumed_spans_off_runtime(staging, hash).await;

        // Until the first byte lands, the entry is discovering holders — surfaced so
        // a slow or stalling probe (a blob no probed node answers for) does not look
        // like a frozen empty bar. The delivery callback switches the row to its
        // download counts on the first chunk.
        if let Some(f) = file {
            f.set_discovering();
        }

        // Plan the reassembly only when there is something to dedup against: hints
        // and a known total. The finalized-staging fast path above already
        // returned, so `staging` does not exist here. A chunk already materialized
        // (a pre-seeded on-disk donor, or a sibling that already finished) becomes a
        // splice `donor`; a chunk this run assigns to another entry becomes
        // `deferred` (waited for at the tail); the rest is driven. When neither a
        // donor nor a deferral applies, there is nothing to dedup — fall through to
        // the plain whole-file fetch.
        let plan = match (hints, total) {
            (Some(hints), Some(total)) if !hints.is_empty() => {
                let guard = index.map.lock().unwrap_or_else(PoisonError::into_inner);
                let plan = plan_reassembly(hints, &guard, fetch_plan, hash, total);
                drop(guard);
                (!plan.donor.is_empty() || !plan.deferred.is_empty()).then_some((plan, total))
            }
            _ => None,
        };

        let Some((plan, total)) = plan else {
            // No donor overlap and nothing deferred — pay for the whole file, then
            // register its chunks so a *later* entry can dedup against it. The
            // manifest's `size`, when it gives one, is the entry's first size claim,
            // a hint; the paid count is the verified blob's length.
            let refetched = self
                .fetch_to_staging(hash, staging, total, progress, slot)
                .await?;
            index.register(hints, staging);
            let len = tokio::fs::metadata(staging).await.map_or(0, |m| m.len());
            let resumed = if refetched { 0 } else { span_bytes(&resumed) };
            return Ok(EntryBytes::whole_blob(len, resumed));
        };

        // Dedup path. The group's `slot` covers every drive below; the entry
        // gives it back only while it waits for a sibling's donor chunks
        // ([`RangeDriver::release_slot`]), and each drive takes it again.
        slot.ensure().await?;

        // Resolve the entry's holders ONCE and reuse them across every drive
        // below (complement, donor re-fetch, whole-blob re-drive), so the entry
        // probes exactly once.
        let targets = self.entry_targets(hash).await?;
        let driver = AcquireRangeDriver {
            ctx: self,
            targets: &targets,
            hash,
            staging,
            total,
            progress,
            refetched: AtomicBool::new(false),
            slot,
        };

        // On any dedup-path success, true up the file + total progress bars to
        // 100%: donor bytes are spliced from disk and never flow through `drive`'s
        // progress callback, so a mostly-spliced entry would otherwise leave its
        // bars short of the blob's full size. Both bars are monotonic, so a path
        // that already reached 100% (a whole-blob re-drive) is unaffected.
        let finish_progress = || {
            if let Some(cb) = progress {
                cb(total, total);
            }
        };

        let outcome = reassemble_dedup(
            &driver,
            &plan,
            total,
            &resumed,
            hints,
            index,
            fetch_plan,
            file,
            &finish_progress,
        )
        .await?
        .refetched_if(driver.refetched.load(Ordering::Relaxed));
        self.dedup_stats
            .spliced_bytes
            .fetch_add(outcome.spliced_bytes, Ordering::Relaxed);
        self.dedup_stats
            .hints_ignored
            .fetch_add(outcome.hints_ignored, Ordering::Relaxed);
        Ok(EntryBytes::dedup(total, outcome))
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
        out_root: &Path,
        overwrite: bool,
        index: &ChunkIndex,
        fetch_plan: &FetchPlan,
        disk: &DiskState,
    ) -> GroupRun {
        // The group's shared hash is carried explicitly; parse it once, and a bad
        // hash fails every path in the group. Then wait for the group's `--jobs`
        // slot: every group of the run is in flight at once, and this is the cap.
        let slot = match fetch::parse_hash(group.hash) {
            Ok(h) => FetchSlot::acquire(&self.gate).await.map(|slot| (h, slot)),
            Err(e) => Err(e),
        };
        let (hash, slot) = match slot {
            Ok(ok) => ok,
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

        // Every entry in the group shares this blob (same whole-file hash), so its
        // chunk decomposition is identical; take the first entry's hints. `None`
        // (no `chunks`, an unparseable chunk hash, or chunk sizes that do not sum
        // to the whole-file size) drops back to a plain whole-file fetch.
        let first = group.entries.first();
        let hints = first.and_then(|e| hints_of(e));
        // A first entry that carries `chunks` but yields no usable hints had its
        // hint set dropped by a fault — an unparseable chunk hash, or chunk sizes
        // that do not sum to the whole-file size. Report the dropped hints so the
        // outcome is not silent; the whole-file `hash` still fetches the blob.
        if let Some(e) = first
            && hints.is_none()
            && let Some(chunks) = e.chunks.as_ref()
        {
            self.dedup_stats.hints_ignored.fetch_add(
                u64::try_from(chunks.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        let total = group.entries.iter().find_map(|e| e.size);
        // An entry that registers chunks can serve as a donor, so its finalized
        // staging blob must outlive this group's cleanup — the run-end sweep in
        // `pull_all` removes it once every recipient has had its chance.
        let is_donor = hints.as_ref().is_some_and(|h| !h.is_empty());

        let slots = plan_slots(&group.entries, out_root, overwrite, &disk.skip);

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
            return GroupRun::done(materialize_from_donor(slots, donor, len).await);
        }

        // Staged once per group: the entry's fetch finalizes to
        // `out_root/.decdn-partial/<hex>` (#1497), streaming its `<hex>.partial`
        // + sidecars there — rather than buffering the blob — which is what lets a
        // large entry top up mid-fetch. The staging path derived from `out_root`
        // is only created once a group actually has something to write, never for
        // an all-skipped group.
        let staging = match staging_path(out_root, hash) {
            Ok(p) => p,
            Err(e) => return GroupRun::done(fail_all(slots, &e)),
        };

        // One per-file bar for this group's single pull, labeled by its destination
        // path(s) — a fetch-once hash group shows one bar for every path it lands at.
        // The bar meters the blob's download (pay-now) bytes and shows its
        // reconstruct (spliced-from-disk) bytes in parentheses; both come from the
        // static plan against the run's fetch plan (an empty donor index — the
        // fresh-pull split), matching the header's download total.
        let paths: Vec<String> = group.entries.iter().map(|e| e.path.clone()).collect();
        let (download_bytes_of_blob, reconstruct_bytes_of_blob) =
            blob_download_reconstruct(&group, fetch_plan, &HashMap::new());
        let file_bar = self.progress.file_bar(
            &pull_progress::file_label(&paths),
            download_bytes_of_blob,
            reconstruct_bytes_of_blob,
        );
        let fetched = self
            .pull_entry(
                hash,
                hints.as_deref(),
                total,
                &staging,
                index,
                fetch_plan,
                Some(&file_bar),
                &slot,
            )
            .await;
        file_bar.finish();
        let bytes = match fetched {
            Ok(bytes) => bytes,
            Err(e) => {
                // `pull_entry` (whole-file or dedup) leaves the `<hex>.partial` +
                // `.ranges` record in place on error: they are what the
                // next run resumes from rather than re-paying for bytes already landed
                // (same contract as `fetch`'s `<output>.partial` store).
                return GroupRun::fetch_failed(slots, e);
            }
        };

        // A failed first write leaves `staging` in place, so the next writable
        // path retries from it. Off the runtime: a multi-GB copy on this task
        // would stall every other group's streams.
        let outcomes = materialize_group(
            slots,
            |dest| {
                let staging = staging.clone();
                off_runtime("materialize", move || {
                    first_write(&staging, &dest, is_donor)
                })
            },
            link_off_runtime,
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
        //
        // A donor blob (`is_donor`) is a splice source a later entry may still
        // read, so it is kept here regardless and swept once at run end by
        // `pull_all`.
        let any_failed = outcomes
            .iter()
            .any(|o| matches!(o, EntryOutcome::Failed { .. }));
        if !is_donor && !any_failed {
            // A non-donor whose every destination landed: its content is safely on
            // disk (the blob itself usually moved there), so drop what is left of
            // staging now. A failed non-donor keeps its `.partial` resume prefix; a
            // donor is kept for splicing and swept at run end.
            remove_staging_off_runtime(&staging).await;
        } else if is_donor && any_failed {
            // A donor whose blob is fully fetched and paid for but whose
            // materialize failed: retain its finalized `<hex>` from the run-end
            // sweep so a rerun resumes from it instead of re-paying the whole blob.
            index.mark_retained(&staging);
        }

        GroupRun::landed(outcomes, bytes)
    }
}

/// The content bytes one landed entry paid for, spliced from disk, and resumed
/// from an earlier run, as [`PullCtx::pull_entry_untimed`] counts them. No byte
/// counts in two of them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EntryBytes {
    /// Content bytes this run fetched and paid for (see
    /// [`PullCtx::pull_entry_untimed`]).
    paid: u64,
    /// Bytes the entry spliced from a local donor on disk rather than
    /// downloading them itself.
    spliced: u64,
    /// Bytes an earlier, interrupted run fetched into the entry's `.partial`,
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
            spliced: 0,
            resumed,
        }
    }

    /// A range-dedup entry of `total` bytes with `outcome`: every byte it did
    /// not splice or resume, this run paid for.
    const fn dedup(total: u64, outcome: DedupOutcome) -> Self {
        Self {
            paid: total
                .saturating_sub(outcome.spliced_bytes)
                .saturating_sub(outcome.resumed_bytes),
            spliced: outcome.spliced_bytes,
            resumed: outcome.resumed_bytes,
        }
    }
}

/// The byte spans of `staging`'s ranged store that earlier runs already
/// fetched: the present set of its `.partial.ranges` record, as sorted,
/// disjoint `(offset, len)` pairs. The store marks only fetched bytes present,
/// never the donor bytes a splice writes, so these spans are bytes an earlier
/// run paid for. No record (a fresh entry) gives no spans.
///
/// The caller reads this before it drives anything, with `staging` absent, so
/// the read opens the record alone and never hashes a final file. The spans
/// are for reporting only: a record it cannot read logs at `debug` and gives
/// no spans, and the fetch that follows surfaces the fault.
fn resumed_spans(staging: &Path, hash: [u8; 32]) -> Vec<(u64, u64)> {
    let read = || -> anyhow::Result<Vec<(u64, u64)>> {
        let (dir, stem) = fetch::ranged_store_location(staging)?;
        if !ClientRangedStore::has_record(&dir, &stem)? {
            return Ok(Vec::new());
        }
        Ok(ClientRangedStore::open(&dir, &stem, hash)?.present_byte_ranges())
    };
    read().unwrap_or_else(|e| {
        tracing::debug!(
            "could not read the resume record beside {}: {e:#}",
            staging.display()
        );
        Vec::new()
    })
}

/// [`resumed_spans`] on the blocking pool. A failed join gives no spans.
async fn resumed_spans_off_runtime(staging: &Path, hash: [u8; 32]) -> Vec<(u64, u64)> {
    let staging = staging.to_path_buf();
    tokio::task::spawn_blocking(move || resumed_spans(&staging, hash))
        .await
        .unwrap_or_else(|e| {
            tracing::debug!("resume record task: {e}");
            Vec::new()
        })
}

/// The total length of `(offset, len)` spans, saturating. The caller passes
/// disjoint spans, so no byte counts twice.
fn span_bytes(spans: &[(u64, u64)]) -> u64 {
    spans
        .iter()
        .fold(0u64, |acc, &(_, len)| acc.saturating_add(len))
}

/// Log a finished entry's line at `-v` (#2120), so a run's slow entries can be
/// told from its fast ones. See [`entry_done_line`]. A staging file it cannot
/// stat logs its size as 0, after a debug line naming the path and the error.
fn log_entry_done(hash: [u8; 32], staging: &Path, bytes: EntryBytes, elapsed: std::time::Duration) {
    let size = std::fs::metadata(staging).map_or_else(
        |e| {
            tracing::debug!(
                "could not stat {} for its entry line: {e}",
                staging.display()
            );
            0
        },
        |m| m.len(),
    );
    tracing::info!("{}", entry_done_line(hash, size, bytes, elapsed));
}

/// A finished entry's size, the time it took, and its rate over the bytes this
/// run paid for (see [`PullCtx::pull_entry_untimed`]). When the entry resumed
/// bytes from an earlier run or spliced bytes from disk, the line reports each
/// non-zero one apart from the paid bytes (#2189, #2236). The time covers the
/// whole entry: probing, every drive, any splice and the whole-file check.
fn entry_done_line(
    hash: [u8; 32],
    size: u64,
    bytes: EntryBytes,
    elapsed: std::time::Duration,
) -> String {
    let secs = elapsed.as_secs_f64();
    let rate = if secs > 0.0 {
        fetch::fmt_rate(fetch::bytes_as_f64(bytes.paid) / secs)
    } else {
        fetch::fmt_rate(0.0)
    };
    let detail = if bytes.spliced == 0 && bytes.resumed == 0 {
        rate
    } else {
        let mut parts = vec![format!(
            "{} downloaded at {rate}",
            indicatif::HumanBytes(bytes.paid)
        )];
        if bytes.resumed > 0 {
            parts.push(format!("{} resumed", indicatif::HumanBytes(bytes.resumed)));
        }
        if bytes.spliced > 0 {
            parts.push(format!(
                "{} spliced from disk",
                indicatif::HumanBytes(bytes.spliced)
            ));
        }
        parts.join(", ")
    };
    format!(
        "bundle pull: {}: {} in {secs:.1}s ({detail})",
        blake3::Hash::from_bytes(hash).to_hex(),
        indicatif::HumanBytes(size),
    )
}

/// The range-drive step [`reassemble_dedup`] performs against one entry: drive
/// exactly `ranges` into the entry's `.partial` (bao-verified against the
/// whole-file hash), or let the ranged store finalize `staging` when they complete
/// it. Abstracted from the reassembly control flow so that flow — which must treat
/// a drive that finalized the blob as done, rather than open a now-renamed
/// `.partial` — is unit-testable without a live endpoint or pool.
trait RangeDriver {
    /// The entry's authoritative whole-file BLAKE3 hash.
    fn hash(&self) -> [u8; 32];

    /// The entry's finalized staging path (`<out_root>/.decdn-partial/<hex>`); its
    /// `.partial` is what the splice writes into.
    fn staging(&self) -> &Path;

    /// Drive `ranges` for the entry. On return the entry's `staging` is either
    /// finalized (the store completed and renamed `<hex>.partial` -> `<hex>`) or
    /// still a `.partial` the caller splices into.
    fn drive<'a>(
        &'a self,
        ranges: &'a [(u64, u64)],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>>;

    /// Give back the entry's `--jobs` slot while it only waits for a sibling's
    /// donor chunks. A driver that holds no slot keeps the default, which does
    /// nothing.
    fn release_slot(&self) {}

    /// Take the entry's `--jobs` slot again as a donor wait ends
    /// ([`Self::release_slot`]). The default does nothing.
    fn retake_slot(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// The production [`RangeDriver`]: each drive fills its ranges through the
/// acquire loop ([`PullCtx::acquire_entry`]) across the entry's holders, which
/// the entry resolved once.
///
/// Every drive of the entry (the pay-now complement, a donor re-fetch, each
/// deferred fallback, the self-heal re-drive) runs the same loop as a
/// whole-file entry: it stripes the ranges across the holders, a holder that
/// faults cools in the command-wide [`PeerHealth`] and returns, and the drive
/// ends on done, a fault only the user can fix, a unanimous verdict of the
/// holders, or the command's [`StopPolicy`]. The manifest's size is the
/// drive's first claim, a hint: a leg that verifies the final chunk proves
/// the true size. The spliced donor bytes are never marked present in the
/// store, so a drive promotes the entry only once fetched bytes fill the
/// whole store: a resumed store that already held the rest, or the self-heal
/// re-drive of the whole blob.
struct AcquireRangeDriver<'a, 'g, P: Provider + Clone> {
    ctx: &'a PullCtx<'a, P>,
    targets: &'a fetch::ResolvedTargets,
    hash: [u8; 32],
    staging: &'a Path,
    /// The manifest's size: the entry's first size claim.
    total: u64,
    progress: Option<&'a ProgressCallback>,
    /// Set once a drive's finalize failed its hash and the drive fetched the
    /// whole blob again ([`PullCtx::acquire_entry`]).
    refetched: AtomicBool,
    /// The entry's `--jobs` slot: held for each drive, given back while the
    /// entry waits for donors.
    slot: &'a FetchSlot<'g>,
}

impl<P: Provider + Clone> RangeDriver for AcquireRangeDriver<'_, '_, P> {
    fn hash(&self) -> [u8; 32] {
        self.hash
    }

    fn staging(&self) -> &Path {
        self.staging
    }

    fn drive<'a>(
        &'a self,
        ranges: &'a [(u64, u64)],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>> {
        Box::pin(async move {
            self.slot.ensure().await?;
            let refetched = self
                .ctx
                .acquire_entry(
                    self.targets,
                    self.hash,
                    self.staging,
                    Some(self.total),
                    Some(ranges),
                    self.progress,
                )
                .await?;
            if refetched {
                self.refetched.store(true, Ordering::Relaxed);
            }
            Ok(())
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

/// Ensure `staging`'s ranged store exists with its `.partial` at the whole-file
/// size, so a splice can seek and write into it.
///
/// A drive normally creates the store; an entry with nothing to drive (every
/// chunk is a donor or deferred) has no drive, so this creates it. It always
/// makes a real store with its `.ranges` record, never a bare `.partial`: a
/// later drive into the same entry (a donor re-fetch, a deferred fallback, the
/// self-heal re-drive) calls `open_or_create`, which keys resume on the
/// `.ranges` record and truncates a `.partial` that lacks one, which wipes
/// every byte already spliced into it. A store that already has its record (a drive
/// made it, or a resumed run) is reopened as is. The data file is then extended
/// to `total`, sparsely, because the whole-file hash reads its full length.
///
/// It runs on the blocking pool: reopening a store can hash a final file.
async fn ensure_partial_off_runtime(
    staging: &Path,
    hash: [u8; 32],
    total: u64,
) -> anyhow::Result<PathBuf> {
    let staging = staging.to_path_buf();
    tokio::task::spawn_blocking(move || ensure_partial(&staging, hash, total))
        .await
        .map_err(|e| anyhow!("ensure partial task: {e}"))?
}

/// [`ensure_partial_off_runtime`]'s body, on the blocking pool.
fn ensure_partial(staging: &Path, hash: [u8; 32], total: u64) -> anyhow::Result<PathBuf> {
    let (dir, stem) = fetch::ranged_store_location(staging)?;
    ClientRangedStore::open_or_create(&dir, &stem, hash, total)
        .with_context(|| format!("open the ranged store for {}", staging.display()))?;
    let partial = partial_path(staging);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&partial)
        .with_context(|| format!("open {}", partial.display()))?;
    if f.metadata()
        .with_context(|| format!("stat {}", partial.display()))?
        .len()
        < total
    {
        f.set_len(total)
            .with_context(|| format!("size {}", partial.display()))?;
    }
    Ok(partial)
}

/// Reassemble one dedup entry's blob into `staging`: drive the pay-now ranges,
/// splice the donor ranges from disk, reconcile the deferred (sibling-assigned)
/// ranges at the tail, verify the whole-file BLAKE3, and promote — using `driver`
/// for every paid range drive.
///
/// The authoritative gate throughout is the whole-file BLAKE3; a bad or lying hint
/// only ever costs a re-download, never a corrupt output or a spuriously-failed
/// entry. After EVERY range drive (pay-now, a donor re-fetch, a deferred fallback,
/// and the whole-blob re-drive) the store may have completed — `drive` then
/// finalized and renamed `<hex>.partial` -> `<hex>`. Each such point checks
/// `staging.try_exists()` and returns success rather than opening a `.partial` that
/// no longer exists: a resumed run whose prior `.partial` already held every range
/// hits this on the very FIRST pay-now drive.
///
/// The deferred ranges are chunks this run assigned to a smaller sibling
/// ([`build_fetch_plan`]). They are NOT driven up front; the tail loop waits on the
/// [`ChunkIndex`] progress signal and splices each one as its assigned fetcher
/// registers it — so the shared bytes are fetched once, by that sibling, and copied
/// here from disk. The wait is never a fixed timeout (the deferred bytes are exactly
/// what the sibling is still downloading): a deferred range is driven-and-paid here
/// only if its assigned fetcher FINISHES without registering the chunk (a failed
/// fetcher), which keeps the run live and never worse than fetching it directly.
///
/// `finish_progress` trues the file + total bars up to 100% on success: donor bytes
/// are spliced from disk and never flow through `drive`'s progress callback, so a
/// mostly-spliced entry would otherwise leave its bars short.
///
/// `resumed` is the byte spans the entry's store held from an earlier run before
/// this run drove anything ([`resumed_spans`]).
///
/// Returns the entry's [`DedupOutcome`] — bytes actually spliced from disk, bytes
/// resumed from an earlier run, and hints dropped by a fault — for the run-level
/// report. A byte both resumed and spliced counts as spliced. A resume that
/// finalized on the first pay-now drive spliced nothing this run; a self-heal
/// whole-blob re-drive discards every splice, so it reports zero spliced bytes and
/// counts all its donors as ignored.
#[allow(clippy::too_many_arguments)]
async fn reassemble_dedup(
    driver: &dyn RangeDriver,
    plan: &ReassemblePlan,
    total: u64,
    resumed: &[(u64, u64)],
    hints: Option<&[Hint]>,
    index: &ChunkIndex,
    fetch_plan: &FetchPlan,
    file: Option<&pull_progress::FileBar>,
    finish_progress: &dyn Fn(),
) -> anyhow::Result<DedupOutcome> {
    let hash = driver.hash();
    let staging = driver.staging();

    // Pay only for the pay-now ranges (this entry's unique + self-assigned chunks
    // and the partial groups at spliced-run ends); donors are spliced and deferred
    // ranges wait.
    // An entry with nothing to drive (every chunk is a donor or deferred) skips the
    // drive entirely and splices into a freshly-sized `.partial`.
    if !plan.drive.is_empty() {
        driver.drive(&plan.drive).await?;
        // A resumed run may already hold every range in `.partial`, so this first
        // drive can COMPLETE the store: `drive` then ran its whole-file hash
        // against `hash` and renamed `<hex>.partial` -> `<hex>`. The blob is
        // finalized and verified; splicing would open a `.partial` that no longer
        // exists. Register the donor chunks and return. No splice ran this run,
        // so every resumed byte counts as resumed.
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome::of(&[], resumed, total, 0));
        }
    }

    let partial = ensure_partial_off_runtime(staging, hash, total).await?;

    // Verify + splice each initially-available donor range off the executor. A donor
    // whose chunk no longer hashes to its hint (a lying donor hint, or a short read)
    // is re-fetched normally rather than trusted.
    // Each re-fetched donor is a dropped hint.
    let mut ledger = SpliceLedger::default();
    let refetch = splice_off_runtime(&partial, plan.donor.clone(), &mut ledger).await?;
    let mut hints_ignored = u64::try_from(refetch.len()).unwrap_or(u64::MAX);
    if !refetch.is_empty() {
        driver.drive(&refetch).await?;
        ledger.driven.extend_from_slice(&refetch);
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome::of(
                &ledger.spliced_spans(total),
                resumed,
                total,
                hints_ignored,
            ));
        }
    }

    // Tail reconcile: splice each deferred (sibling-assigned) range as its fetcher
    // registers it, waiting on the index's progress signal. A deferred chunk whose
    // assigned fetcher finishes WITHOUT producing it (a failed fetcher) is driven
    // and paid here instead, so the run never hangs.
    let tail_ignored = reconcile_deferred(
        driver,
        &partial,
        &plan.deferred,
        index,
        fetch_plan,
        file,
        &mut ledger,
    )
    .await?;
    hints_ignored = hints_ignored.saturating_add(tail_ignored);
    let mut spliced = ledger.spliced_spans(total);
    if staging.try_exists()? {
        finish_progress();
        index.register(hints, staging);
        return Ok(DedupOutcome::of(&spliced, resumed, total, hints_ignored));
    }

    // The authoritative check: the whole reassembled blob must hash to `hash`. This
    // hash of the full blob is the slow tail of a mostly-spliced entry, so it drives
    // the file row's `reconstructing…` bar with its running byte count — otherwise
    // the row would sit frozen while a multi-GB blob verifies.
    let reporter = file.and_then(pull_progress::FileBar::reconstruct_reporter);
    let partial_for_hash = partial.clone();
    let got = tokio::task::spawn_blocking(move || {
        hash_partial_with_progress(&partial_for_hash, reporter.as_deref())
    })
    .await
    .map_err(|e| anyhow!("whole-file hash task: {e}"))??;
    if got != hash {
        // A lying recipient hint placed a chunk at the wrong offset. Drop the
        // spliced ranges by re-driving the whole blob (the ranged store fetches
        // exactly the bytes the splice wrote, bao-verified against `hash`) and
        // re-verify. Every donor is discarded, so nothing was saved and all of them
        // count as ignored. The resumed bytes stay present in the store, so the
        // re-drive does not fetch them again.
        spliced = Vec::new();
        hints_ignored =
            u64::try_from(plan.donor.len().saturating_add(plan.deferred.len())).unwrap_or(u64::MAX);
        tracing::warn!(
            "bundle pull: entry {} failed its whole-file hash after range-dedup; \
             re-fetching the whole blob",
            blake3::Hash::from_bytes(hash).to_hex()
        );
        driver.drive(&[(0, total)]).await?;
        // The whole-blob re-drive covers `[0, total)`, so `drive` finalized it: its
        // whole-file hash verified the bytes against `hash` and renamed `.partial` ->
        // `staging`. The blob is verified — do not re-hash a `.partial` that no
        // longer exists; register the donor chunks and return.
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome::of(&spliced, resumed, total, hints_ignored));
        }
        let partial_for_hash = partial.clone();
        let got = tokio::task::spawn_blocking(move || hash_partial(&partial_for_hash))
            .await
            .map_err(|e| anyhow!("whole-file hash task: {e}"))??;
        if got != hash {
            bail!(
                "reconstructed blob {} does not match its whole-file hash after a full re-fetch",
                blake3::Hash::from_bytes(hash).to_hex()
            );
        }
    }

    // Promote the verified `.partial` to the plain staging file and clean up the
    // ranged-store sidecars, then register this blob's chunks as donors.
    let partial_for_promote = partial.clone();
    let staging_for_promote = staging.to_path_buf();
    tokio::task::spawn_blocking(move || {
        promote_partial(&partial_for_promote, &staging_for_promote)
    })
    .await
    .map_err(|e| anyhow!("promote task: {e}"))??;
    finish_progress();
    index.register(hints, staging);
    Ok(DedupOutcome::of(&spliced, resumed, total, hints_ignored))
}

/// Tail reconcile for one entry's deferred (sibling-assigned) chunks: splice each
/// as its assigned fetcher registers it in `index`, waiting on the index progress
/// signal between scans rather than polling on a clock. A deferred chunk whose
/// assigned fetcher FINISHES without registering it (a failed fetcher, or one with
/// no recorded assignee) — or one registered under a length that disagrees with the
/// hint — is driven and paid here instead, over the whole groups its span touches,
/// so the entry always completes and the run never hangs. Records each splice and
/// each driven span in `ledger`, and returns the count of deferred chunks that fell
/// back to a paid drive.
async fn reconcile_deferred(
    driver: &dyn RangeDriver,
    partial: &Path,
    deferred: &[DeferredChunk],
    index: &ChunkIndex,
    fetch_plan: &FetchPlan,
    file: Option<&pull_progress::FileBar>,
    ledger: &mut SpliceLedger,
) -> anyhow::Result<u64> {
    let mut waiting: Vec<DeferredChunk> = deferred.to_vec();
    let mut ignored = 0u64;
    if waiting.is_empty() {
        return Ok(0);
    }
    let notified = index.progress.notified();
    tokio::pin!(notified);
    loop {
        // Register as a waiter BEFORE scanning so a registration/finish between the
        // scan and the await is not lost (`notify_waiters` stores no permit).
        notified.as_mut().enable();

        let mut ready: Vec<DonorRange> = Vec::new();
        let mut fallback: Vec<(u64, u64)> = Vec::new();
        let mut still: Vec<DeferredChunk> = Vec::new();
        {
            let guard = index.map.lock().unwrap_or_else(PoisonError::into_inner);
            for c in &waiting {
                let h = &c.hint;
                // A trustworthy donor for this chunk is available now — splice it.
                // A registered length that disagrees with the hint is untrusted.
                if let Some(m) = guard.get(&h.hash).filter(|m| m.len == h.len) {
                    ready.push(DonorRange::new(h, m, c.dst, c.refetch));
                    continue;
                }
                // Otherwise keep waiting ONLY while the assigned fetcher is still
                // running and might yet register a good donor. Stop waiting — and pay
                // for the interior here — once that fetcher has finished without
                // producing it, or a registered donor's length disagreed (present but
                // untrusted), or there is no recorded assignee at all.
                let present_but_untrusted = guard.contains_key(&h.hash);
                let fetcher_running = fetch_plan
                    .assigned
                    .get(&h.hash)
                    .copied()
                    .is_some_and(|a| !index.is_finished(a));
                if fetcher_running && !present_but_untrusted {
                    still.push(*c);
                } else {
                    fallback.push(c.refetch);
                    ignored = ignored.saturating_add(1);
                }
            }
        }

        if !ready.is_empty() {
            let refetch = splice_off_runtime(partial, ready, ledger).await?;
            ignored = ignored.saturating_add(u64::try_from(refetch.len()).unwrap_or(u64::MAX));
            fallback.extend(refetch);
        }

        if !fallback.is_empty() {
            driver.drive(&fallback).await?;
            ledger.driven.extend_from_slice(&fallback);
            // A fallback drive may complete the store (`drive` renames
            // `.partial` -> `<hex>`); no more splicing is then possible or needed.
            if driver.staging().try_exists()? {
                return Ok(ignored);
            }
        }

        waiting = still;
        if waiting.is_empty() {
            return Ok(ignored);
        }

        // Nothing to do this pass but wait on a sibling — surface it on the file row
        // (its download is done; the total bar shows the run is still moving).
        if let Some(f) = file {
            f.set_pending();
        }
        // Block until the next registration or group-finish, then rescan. A
        // waiter moves no bytes, so its `--jobs` slot goes to a queued entry
        // meanwhile; a fallback drive takes one again.
        driver.release_slot();
        notified.as_mut().await;
        driver.retake_slot().await?;
        notified.set(index.progress.notified());
    }
}

/// The run-end sweep of donor staging blobs: remove every registered donor
/// source EXCEPT one [`ChunkIndex::mark_retained`] flagged (its blob is paid for
/// but a destination failed to materialize, so its finalized `<hex>` is the resume
/// prefix a rerun needs — deleting it would force a full re-fetch and re-payment).
/// The removal runs on the blocking pool (see [`remove_staging_off_runtime`]).
async fn sweep_donor_sources(index: &ChunkIndex) {
    let doomed: Vec<PathBuf> = index
        .sources()
        .into_iter()
        .filter(|source| !index.retained(source))
        .collect();
    if let Err(e) = tokio::task::spawn_blocking(move || {
        for source in &doomed {
            remove_staging(source);
        }
    })
    .await
    {
        tracing::warn!("donor staging sweep task: {e}");
    }
}

/// Copy the already-fetched, already-verified blob at `staging` to `dest`,
/// returning the byte count. `staging` is read-only here (not consumed) — a
/// retry after a failed first write calls this again for the next writable
/// path — so the copy goes through a unique temp file beside `dest` and an
/// atomic rename, the same source-must-survive shape [`link_or_copy_atomic`]
/// uses for every later duplicate — built from `fetch::temp_in_parent`, the
/// same staging primitive `fetch`'s single-blob path used to build its own
/// atomic writer from.
fn materialize(staging: &Path, dest: &Path) -> anyhow::Result<u64> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut tmp =
        fetch::temp_in_parent(dest).with_context(|| format!("stage {}", dest.display()))?;
    let mut src =
        std::fs::File::open(staging).with_context(|| format!("open {}", staging.display()))?;
    let written = std::io::copy(&mut src, tmp.as_file_mut())
        .with_context(|| format!("copy {} -> {}", staging.display(), dest.display()))?;
    tmp.as_file()
        .sync_all()
        .with_context(|| format!("sync staged copy for {}", dest.display()))?;
    tmp.persist(dest)
        .map_err(|e| e.error)
        .with_context(|| format!("write {}", dest.display()))?;
    Ok(written)
}

/// Land a group's finalized `staging` blob at its first destination, returning
/// its byte count: a non-donor moves ([`move_into_place`]); a donor — a splice
/// source the run-end sweep still owns — is hard-linked (a copy only across
/// filesystems), so its staging name survives for later recipients.
fn first_write(staging: &Path, dest: &Path, is_donor: bool) -> anyhow::Result<u64> {
    if !is_donor {
        return move_into_place(staging, dest);
    }
    let len = std::fs::metadata(staging)
        .with_context(|| format!("stat {}", staging.display()))?
        .len();
    link_or_copy_atomic(staging, dest).map(|()| len)
}

/// Move the finalized `staging` blob to `dest`, returning its byte count. A
/// non-donor blob has no reader after its first destination lands, so a rename
/// replaces the full copy [`materialize`] makes — no second write of the blob and
/// no second copy on disk. `staging` is already durable: the ranged store's
/// finalize, and the dedup splice before promotion, sync it. A rename that fails
/// (a destination on another filesystem, `EXDEV`) falls back to [`materialize`],
/// which leaves `staging` in place for the next writable path.
fn move_into_place(staging: &Path, dest: &Path) -> anyhow::Result<u64> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let len = std::fs::metadata(staging)
        .with_context(|| format!("stat {}", staging.display()))?
        .len();
    match std::fs::rename(staging, dest) {
        Ok(()) => Ok(len),
        Err(_) => materialize(staging, dest),
    }
}

/// One chunk of a hint-carrying entry, resolved to an absolute byte placement in
/// the whole file: `offset` is the running sum of prior chunk sizes and `len` the
/// chunk's own size, so `[offset, offset + len)` is the chunk's span.
#[derive(Debug, Clone, Copy)]
struct Hint {
    /// The chunk's BLAKE3 content address.
    hash: [u8; 32],
    /// Byte offset of the chunk within the whole file.
    offset: u64,
    /// Chunk length in bytes.
    len: u64,
}

/// Where one chunk's verified bytes are already materialized on disk this run.
struct MaterializedRange {
    /// The finalized staging file (a completed entry's whole-file blob) that
    /// contains the chunk.
    source: PathBuf,
    /// Byte offset of the chunk within `source`.
    offset: u64,
    /// Chunk length in bytes.
    len: u64,
}

/// In-run index from a chunk's BLAKE3 hash to where its verified bytes live on
/// disk. Shared across the run's concurrent entries behind a mutex; an entry
/// registers its chunks only after it fully completes and its staging blob is
/// finalized, so a donor is always fully written before a recipient reads it.
#[derive(Default)]
struct ChunkIndex {
    /// First-writer-wins map; a chunk registered by one completed entry serves
    /// every later entry that shares it.
    map: std::sync::Mutex<HashMap<[u8; 32], MaterializedRange>>,
    /// Donor staging blobs the run-end sweep in [`PullCtx::pull_all`] must NOT
    /// delete: a donor group whose blob is fully fetched and registered but
    /// whose materialize to disk failed (ENOSPC, an unwritable dest). Its
    /// finalized `<hex>` is the resume prefix a rerun needs — deleting it would
    /// force a full re-fetch and re-payment of an unrefunded blob.
    retain: std::sync::Mutex<HashSet<PathBuf>>,
    /// Output-file donor sources seeded from disk before the run. Excluded from
    /// [`Self::sources`] so the run-end staging sweep never deletes a
    /// materialized output.
    seeded: std::sync::Mutex<HashSet<PathBuf>>,
    /// Whole-file hashes of groups whose fetch has FINISHED this run, success or
    /// failure. A tail-reconcile waiter watches this: once the entry a deferred
    /// chunk is assigned to has finished without registering that chunk, the waiter
    /// stops waiting and drives the range itself (the assigned fetcher failed). It
    /// only ever grows.
    finished: std::sync::Mutex<HashSet<[u8; 32]>>,
    /// Pinged whenever a donor is registered/seeded or a group finishes, so a
    /// tail-reconcile waiter re-checks its deferred chunks without polling.
    progress: tokio::sync::Notify,
}

impl ChunkIndex {
    /// Register a completed entry's chunks, all pointing at its finalized
    /// `source` blob. First writer wins, so a chunk shared by several entries
    /// keeps the first donor. A `None` hint list (a plain entry) registers
    /// nothing.
    fn register(&self, hints: Option<&[Hint]>, source: &Path) {
        let Some(hints) = hints else {
            return;
        };
        {
            let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
            for h in hints {
                map.entry(h.hash).or_insert_with(|| MaterializedRange {
                    source: source.to_path_buf(),
                    offset: h.offset,
                    len: h.len,
                });
            }
        }
        // New donors may unblock a tail-reconcile waiter.
        self.progress.notify_waiters();
    }

    /// The distinct donor staging blobs registered this run, for the run-end
    /// sweep in [`PullCtx::pull_all`]. Excludes any [`Self::seed_disk`] source —
    /// an on-disk OUTPUT file the sweep must never delete.
    fn sources(&self) -> Vec<PathBuf> {
        // Acquire `map` before `seeded` — the single lock order every site follows
        // (`register`/`seed_disk` touch `map` first), so no pair of these methods
        // can ever invert and deadlock.
        let map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        let seeded = self.seeded.lock().unwrap_or_else(PoisonError::into_inner);
        let mut seen: HashSet<&Path> = HashSet::new();
        let mut out = Vec::new();
        for m in map.values() {
            if !seeded.contains(&m.source) && seen.insert(m.source.as_path()) {
                out.push(m.source.clone());
            }
        }
        out
    }

    /// Seed a donor whose bytes live in an on-disk OUTPUT file (a skipped
    /// entry's current file, or a changed entry's old copy still present until
    /// its atomic materialize). First-writer-wins, like [`Self::register`]; the
    /// source is recorded as seeded so the sweep never removes it. The existing
    /// per-chunk re-hash guard verifies the bytes before any splice trusts them,
    /// so a stale offset or edited/removed file simply falls back to a fetch.
    fn seed_disk(&self, hash: [u8; 32], source: &Path, offset: u64, len: u64) {
        {
            let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
            map.entry(hash).or_insert_with(|| MaterializedRange {
                source: source.to_path_buf(),
                offset,
                len,
            });
        }
        {
            let mut seeded = self.seeded.lock().unwrap_or_else(PoisonError::into_inner);
            seeded.insert(source.to_path_buf());
        }
        // A pre-seeded on-disk donor may unblock a tail-reconcile waiter.
        self.progress.notify_waiters();
    }

    /// Record that a group's fetch has finished this run (success or failure) and
    /// wake any tail-reconcile waiter. A waiter watching a deferred chunk assigned
    /// to `whole` stops waiting once `whole` is finished but the chunk never
    /// registered — the assigned fetcher failed to produce it, so the waiter drives
    /// the range itself.
    fn mark_finished(&self, whole: [u8; 32]) {
        {
            let mut finished = self.finished.lock().unwrap_or_else(PoisonError::into_inner);
            finished.insert(whole);
        }
        self.progress.notify_waiters();
    }

    /// Whether the group with whole-file hash `whole` has finished fetching this
    /// run (see [`Self::mark_finished`]).
    fn is_finished(&self, whole: [u8; 32]) -> bool {
        self.finished
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&whole)
    }

    /// Mark a donor `source` as a resume prefix the run-end sweep must keep: its
    /// blob is fully fetched and paid for, but at least one destination failed to
    /// materialize, so a rerun needs the finalized `<hex>` rather than re-paying.
    fn mark_retained(&self, source: &Path) {
        let mut retain = self.retain.lock().unwrap_or_else(PoisonError::into_inner);
        retain.insert(source.to_path_buf());
    }

    /// Whether `source` was retained by [`Self::mark_retained`] and so must
    /// survive the run-end sweep.
    fn retained(&self, source: &Path) -> bool {
        let retain = self.retain.lock().unwrap_or_else(PoisonError::into_inner);
        retain.contains(source)
    }
}

/// One splice source for the dedup path: the part of one chunk of the recipient
/// blob that its spliced run covers, and the materialized donor that holds it.
#[derive(Debug, Clone)]
struct DonorRange {
    /// `(offset, len)` in the recipient blob to write: the chunk's span inside its
    /// spliced run ([`plan_reassembly`]). It need not sit on chunk-group
    /// boundaries — adjacent spliced chunks tile a run whose ends do, so the runs
    /// and the ranged store's complement (fetched on group boundaries) tile the
    /// blob without overlap.
    dst: (u64, u64),
    /// The whole chunk groups `dst` touches, capped at the blob end — what a paid
    /// re-fetch drives when this donor cannot be trusted. It may take in a
    /// neighbour's bytes of a shared boundary group; the drive writes verified
    /// bytes over them.
    refetch: (u64, u64),
    /// The donor blob to read from.
    source: PathBuf,
    /// Source offset of the `dst` span within `source`.
    src_offset: u64,
    /// The whole chunk's hash — the `dst` span is trusted only after the
    /// whole chunk at `[chunk_src_offset, chunk_src_offset + chunk_len)` in
    /// `source` re-hashes to this.
    chunk_hash: [u8; 32],
    /// Whole chunk start in `source` (for the verification re-hash).
    chunk_src_offset: u64,
    /// Whole chunk length in bytes (for the verification re-hash).
    chunk_len: u64,
}

/// Turn an entry's optional chunk list into placed [`Hint`]s, or `None` when the
/// entry should be fetched as a plain whole-file blob: no `chunks`, no whole-file
/// `size` to validate against, an unparseable chunk hash, or chunk sizes that do
/// not sum to the whole-file `size` (a malformed hint set, logged at debug). The
/// whole-file `hash` is authoritative in every case, so ignoring hints only ever
/// costs dedup, never correctness.
fn hints_of(entry: &ManifestEntry) -> Option<Vec<Hint>> {
    let chunks = entry.chunks.as_ref()?;
    let total = entry.size?;
    let mut hints = Vec::with_capacity(chunks.len());
    let mut offset = 0u64;
    for c in chunks {
        let hash = fetch::parse_hash(&c.hash).ok()?;
        hints.push(Hint {
            hash,
            offset,
            len: c.size,
        });
        offset = offset.checked_add(c.size)?;
    }
    if offset != total {
        tracing::debug!(
            "bundle entry {:?}: chunk sizes sum to {offset}, expected whole-file size {total}; \
             ignoring range-dedup hints",
            entry.path
        );
        return None;
    }
    Some(hints)
}

impl DonorRange {
    /// The splice of chunk `h`'s span `dst` from the materialized donor `m`,
    /// which holds the whole chunk at `m.offset`; `refetch` is `dst`'s
    /// [`outward_groups`].
    fn new(h: &Hint, m: &MaterializedRange, dst: (u64, u64), refetch: (u64, u64)) -> Self {
        Self {
            dst,
            refetch,
            source: m.source.clone(),
            src_offset: m.offset.saturating_add(dst.0.saturating_sub(h.offset)),
            chunk_hash: h.hash,
            chunk_src_offset: m.offset,
            chunk_len: m.len,
        }
    }
}

/// A deferred chunk of a [`ReassemblePlan`]: a chunk this run assigns to another
/// entry, spliced over `dst` once that entry registers it, or driven over
/// `refetch` (see [`DonorRange`]) if it never does.
#[derive(Debug, Clone, Copy)]
struct DeferredChunk {
    hint: Hint,
    dst: (u64, u64),
    refetch: (u64, u64),
}

/// The whole chunk groups the byte span `(offset, len)` touches, as
/// `(offset, len)`, capped at `total`.
fn outward_groups((offset, len): (u64, u64), total: u64) -> (u64, u64) {
    let group = CHUNK_GROUP_BYTES;
    let start = (offset / group).saturating_mul(group);
    let end = offset
        .saturating_add(len)
        .div_ceil(group)
        .saturating_mul(group)
        .min(total);
    (start, end.saturating_sub(start))
}

/// The run-level shared-chunk fetch plan: which entry pays to fetch each chunk that
/// appears in two or more entry positions across the whole manifest. A chunk in one
/// position only is absent — its sole entry pays for it as part of its complement.
/// Built once per run, so every entry derives the same assignment with no
/// coordination.
#[derive(Debug, Default)]
struct FetchPlan {
    /// Shared chunk hash → the whole-file hash of the entry assigned to fetch it.
    /// The assignee is the smallest containing entry by whole-file size, tie-broken
    /// by whole-file hash bytes, so the fetcher finishes soonest and every waiter is
    /// a larger entry whose own tail comes later.
    assigned: HashMap<[u8; 32], [u8; 32]>,
}

/// Build the run-level [`FetchPlan`]: assign every chunk shared by two or more entry
/// positions to its smallest containing entry (tie-break by whole-file hash), so a
/// shared chunk is fetched and paid for exactly once per run and spliced into every
/// other entry that names it. An entry without usable hints or an unparseable
/// whole-file hash takes no part. An entry with no declared `size` ranks as
/// `u64::MAX`, so a sized sharer — the only kind that can be spliced from anyway —
/// always wins the assignment.
fn build_fetch_plan(entries: &[ManifestEntry]) -> FetchPlan {
    struct Occurrence {
        count: u32,
        best_size: u64,
        best_whole: [u8; 32],
    }
    let mut seen: HashMap<[u8; 32], Occurrence> = HashMap::new();
    for e in entries {
        let Ok(whole) = fetch::parse_hash(&e.hash) else {
            continue;
        };
        let Some(hints) = hints_of(e) else {
            continue;
        };
        let size = e.size.unwrap_or(u64::MAX);
        for h in &hints {
            seen.entry(h.hash)
                .and_modify(|o| {
                    o.count = o.count.saturating_add(1);
                    if (size, whole) < (o.best_size, o.best_whole) {
                        o.best_size = size;
                        o.best_whole = whole;
                    }
                })
                .or_insert(Occurrence {
                    count: 1,
                    best_size: size,
                    best_whole: whole,
                });
        }
    }
    let assigned = seen
        .into_iter()
        .filter(|(_, o)| o.count >= 2)
        .map(|(hash, o)| (hash, o.best_whole))
        .collect();
    FetchPlan { assigned }
}

/// One entry's reassembly plan, computed from the live [`ChunkIndex`] snapshot and
/// the run-level [`FetchPlan`]. The three range sets tile the blob:
///
/// - `donor`: chunks already materialized somewhere (a pre-seeded on-disk file, or
///   a sibling that already finished) — spliced immediately, no fetch.
/// - `deferred`: chunks assigned to a DIFFERENT entry and not yet available —
///   waited for and spliced at the tail once the assigned fetcher registers them
///   (or driven as a fallback if that fetcher finishes without producing them).
/// - `drive`: everything else — this entry's unique and self-assigned chunks, plus
///   the partial chunk groups at the edges of each spliced run — driven and paid
///   for now.
///
/// Donor and deferred chunks that touch form one spliced run, and only the run's
/// two ends round inward to chunk-group boundaries. A boundary between two
/// spliced chunks therefore costs nothing, wherever it falls: its group is
/// spliced from both sides rather than driven as a separate 16 KiB request.
///
/// With an empty [`FetchPlan`] (no shared chunks) `deferred` is empty and the plan
/// reduces to a donor/complement split.
struct ReassemblePlan {
    donor: Vec<DonorRange>,
    deferred: Vec<DeferredChunk>,
    drive: Vec<(u64, u64)>,
}

/// Build an entry's [`ReassemblePlan`] for the entry whose whole-file hash is
/// `whole`. A chunk currently in `index` under its hinted length becomes a
/// `donor` (spliced now); a chunk the [`FetchPlan`] assigns to another entry, not
/// yet available, becomes `deferred`. Their spans merge into runs
/// ([`spliced_runs`]); a chunk with no bytes inside a run is dropped to the
/// drive, and everything outside the runs is driven. `total` is the entry's
/// whole-file size.
fn plan_reassembly(
    hints: &[Hint],
    index: &HashMap<[u8; 32], MaterializedRange>,
    fetch_plan: &FetchPlan,
    whole: [u8; 32],
    total: u64,
) -> ReassemblePlan {
    // Each candidate chunk, with its donor when one is materialized now.
    let mut candidates: Vec<(&Hint, Option<&MaterializedRange>)> = Vec::new();
    for h in hints {
        match index.get(&h.hash) {
            // A donor whose claimed chunk length disagrees with the hint is one
            // side's manifest lying about the chunk — never spliced from.
            Some(m) if m.len == h.len => candidates.push((h, Some(m))),
            _ => {
                let assigned_elsewhere = fetch_plan
                    .assigned
                    .get(&h.hash)
                    .is_some_and(|owner| *owner != whole);
                if assigned_elsewhere {
                    candidates.push((h, None));
                }
            }
        }
    }
    let spans: Vec<(u64, u64)> = candidates.iter().map(|(h, _)| (h.offset, h.len)).collect();
    let runs = spliced_runs(&spans, total);

    let mut donor = Vec::new();
    let mut deferred = Vec::new();
    for (h, m) in candidates {
        let Some(dst) = span_in_runs(&runs, h.offset, h.len) else {
            continue;
        };
        match m {
            Some(m) => donor.push(DonorRange::new(h, m, dst, outward_groups(dst, total))),
            None => deferred.push(DeferredChunk {
                hint: *h,
                dst,
                refetch: outward_groups(dst, total),
            }),
        }
    }
    let covered: Vec<(u64, u64)> = runs.iter().map(|&(s, e)| (s, e - s)).collect();
    ReassemblePlan {
        drive: complement_runs(&covered, total),
        donor,
        deferred,
    }
}

/// Merge the spliceable chunk spans `(offset, len)` into sorted, disjoint
/// `(start, end)` runs — spans that overlap or touch join one run — then round
/// each run inward to chunk-group boundaries, the blob end counting as one. A
/// run with no whole group left is dropped. The ranged store fetches whole
/// groups, so only a run's two ends can leave a partial group to drive.
fn spliced_runs(spans: &[(u64, u64)], total: u64) -> Vec<(u64, u64)> {
    let group = CHUNK_GROUP_BYTES;
    coalesce_runs(spans, total)
        .into_iter()
        .filter_map(|(start, end)| {
            let start = start.div_ceil(group).saturating_mul(group);
            let end = if end == total {
                end
            } else {
                (end / group).saturating_mul(group)
            };
            (start < end).then_some((start, end))
        })
        .collect()
}

/// The part of the span `(offset, len)` inside the sorted, disjoint `runs`, as
/// `(offset, len)`, or `None` when no run covers any of it. A span from the same
/// set the runs were built from meets at most one run.
fn span_in_runs(runs: &[(u64, u64)], offset: u64, len: u64) -> Option<(u64, u64)> {
    let end = offset.saturating_add(len);
    let i = runs.partition_point(|&(_, run_end)| run_end <= offset);
    let &(run_start, run_end) = runs.get(i)?;
    let (start, end) = (offset.max(run_start), end.min(run_end));
    (start < end).then_some((start, end - start))
}

/// The `.partial` data file the ranged store keeps beside a finalized `staging`
/// blob — `<hex>.partial` — where driven ranges and spliced donor bytes land
/// before promotion.
fn partial_path(staging: &Path) -> PathBuf {
    staging.with_extension("partial")
}

/// Verify and splice every donor range into `partial`, returning the donors that
/// could NOT be trusted (a donor whose chunk no longer hashes to its hint, or an
/// unreadable source): the caller re-fetches and pays for each one's
/// [`DonorRange::refetch`] span.
///
/// A donor's bytes are trusted only after the WHOLE chunk at its recorded source
/// offset re-hashes to the chunk hash — a donor's own chunk placement is an
/// unverified manifest claim until then. The trusted `dst` span is then written
/// at the recipient offset; the whole-file BLAKE3 the caller runs next is the
/// authoritative backstop against a lying recipient placement. A span that lands
/// in a group the ranged store already holds (a resumed run, or a neighbour's
/// re-fetch) writes the same verified bytes; if a lying placement wrote others,
/// the store's finalize verify drops that group and a drive fetches it again.
fn splice_donors(partial: &Path, donors: &[DonorRange]) -> anyhow::Result<Vec<DonorRange>> {
    use std::io::{Seek, SeekFrom};

    let mut refetch = Vec::new();
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .open(partial)
        .with_context(|| format!("open {}", partial.display()))?;
    for d in donors {
        let (dst_offset, len) = d.dst;
        // An unreadable donor source is not trusted — pay to fetch it.
        let Ok(mut src) = std::fs::File::open(&d.source) else {
            refetch.push(d.clone());
            continue;
        };
        if !chunk_verified(&mut src, d.chunk_src_offset, d.chunk_len, d.chunk_hash) {
            refetch.push(d.clone());
            continue;
        }
        // The chunk is confirmed present at `chunk_src_offset`; copy its `dst`
        // span into the recipient's `.partial`. A seek/copy error here (a
        // truncated or racing donor file, an I/O fault) is not fatal: the donor
        // bytes are simply not trusted, so queue the donor for a paid,
        // bao-verified re-fetch that overwrites every group it touches — nothing
        // torn by a partial copy survives, and the whole-file BLAKE3 the caller
        // runs next is the authoritative backstop.
        let copied = src
            .seek(SeekFrom::Start(d.src_offset))
            .and_then(|_| out.seek(SeekFrom::Start(dst_offset)))
            .and_then(|_| copy_exact(&mut src, &mut out, len));
        if copied.is_err() {
            refetch.push(d.clone());
        }
    }
    out.sync_all()
        .with_context(|| format!("sync {}", partial.display()))?;
    Ok(refetch)
}

/// [`splice_donors`] on the blocking pool. Records the `dst` span of each donor
/// it spliced in `ledger`, and returns the [`DonorRange::refetch`] span of each
/// untrusted donor, which the caller drives and pays for.
async fn splice_off_runtime(
    partial: &Path,
    donors: Vec<DonorRange>,
    ledger: &mut SpliceLedger,
) -> anyhow::Result<Vec<(u64, u64)>> {
    let partial = partial.to_path_buf();
    let planned: Vec<((u64, u64), (u64, u64))> =
        donors.iter().map(|d| (d.dst, d.refetch)).collect();
    let failed = off_runtime("donor splice", move || splice_donors(&partial, &donors)).await?;
    let failed_dst: HashSet<(u64, u64)> = failed.iter().map(|d| d.dst).collect();
    ledger.spliced.extend(
        planned
            .iter()
            .filter(|(dst, _)| !failed_dst.contains(dst))
            .map(|&(dst, _)| dst),
    );
    Ok(failed.iter().map(|d| d.refetch).collect())
}

/// The byte spans one entry's reassembly spliced from disk and the spans it
/// drove to replace an untrusted or missing donor. A driven span is widened to
/// whole chunk groups, so it can cover part of a neighbour's spliced span, and
/// the drive then writes and pays for those bytes.
#[derive(Debug, Default)]
struct SpliceLedger {
    /// The `dst` span of each donor that spliced cleanly.
    spliced: Vec<(u64, u64)>,
    /// Each re-fetch or fallback span that a drive covered.
    driven: Vec<(u64, u64)>,
}

impl SpliceLedger {
    /// The spans served from a splice: the spliced spans less every driven
    /// span, as sorted, disjoint `(offset, len)` pairs.
    fn spliced_spans(&self, total: u64) -> Vec<(u64, u64)> {
        uncovered_runs(&self.spliced, &self.driven, total)
    }
}

/// The parts of `spans` that no `cover` span covers, over `[0, total)`, as
/// sorted, disjoint `(offset, len)` pairs. Both inputs are `(offset, len)`
/// spans, in any order, and may overlap.
fn uncovered_runs(spans: &[(u64, u64)], cover: &[(u64, u64)], total: u64) -> Vec<(u64, u64)> {
    let kept = complement_runs(cover, total);
    let mut kept = kept
        .iter()
        .map(|&(offset, len)| (offset, offset.saturating_add(len)));
    let mut current = kept.next();
    let mut out = Vec::new();
    for (start, end) in coalesce_runs(spans, total) {
        while let Some((k_start, k_end)) = current {
            if k_end <= start {
                current = kept.next();
                continue;
            }
            if k_start >= end {
                break;
            }
            let (from, to) = (start.max(k_start), end.min(k_end));
            if to > from {
                out.push((from, to - from));
            }
            if k_end <= end {
                current = kept.next();
            } else {
                break;
            }
        }
    }
    out
}

/// Whether the `len` bytes at `offset` in `src` hash to `expected`. Any read
/// failure (a short source, an I/O error) returns `false`: the bytes are simply
/// not trusted, and the caller re-fetches them rather than splicing.
fn chunk_verified(src: &mut std::fs::File, offset: u64, len: u64, expected: [u8; 32]) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    if src.seek(SeekFrom::Start(offset)).is_err() {
        return false;
    }
    let mut hasher = blake3::Hasher::new();
    let mut remaining = len;
    let mut buf = vec![0u8; 1 << 20];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(1 << 20)).unwrap_or(1 << 20);
        let Some(slice) = buf.get_mut(..want) else {
            return false;
        };
        if src.read_exact(slice).is_err() {
            return false;
        }
        hasher.update(slice);
        remaining = remaining.saturating_sub(u64::try_from(want).unwrap_or(remaining));
    }
    *hasher.finalize().as_bytes() == expected
}

/// Copy exactly `len` bytes from `src` to `out`, both already positioned, in
/// bounded chunks so a large range never loads into memory at once.
fn copy_exact(src: &mut std::fs::File, out: &mut std::fs::File, len: u64) -> std::io::Result<()> {
    use std::io::{Read, Write};
    let mut remaining = len;
    let mut buf = vec![0u8; 1 << 20];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(1 << 20)).unwrap_or(1 << 20);
        let slice = buf
            .get_mut(..want)
            .ok_or_else(|| std::io::Error::other("copy buffer slice"))?;
        src.read_exact(slice)?;
        out.write_all(slice)?;
        remaining = remaining.saturating_sub(u64::try_from(want).unwrap_or(remaining));
    }
    Ok(())
}

/// BLAKE3 of the whole `partial` data file, streamed so an arbitrarily large blob
/// never loads into memory at once.
fn hash_partial(partial: &Path) -> anyhow::Result<[u8; 32]> {
    hash_partial_with_progress(partial, None)
}

/// BLAKE3 of the whole `partial` data file, streamed, reporting the cumulative bytes
/// hashed to `progress` after each read — so a caller can advance a progress bar
/// through a multi-GB verify instead of showing a frozen row.
fn hash_partial_with_progress(
    partial: &Path,
    progress: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> anyhow::Result<[u8; 32]> {
    use std::io::Read;
    let mut f =
        std::fs::File::open(partial).with_context(|| format!("open {}", partial.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut hashed_bytes = 0u64;
    loop {
        let n = f
            .read(&mut buf)
            .with_context(|| format!("read {}", partial.display()))?;
        if n == 0 {
            break;
        }
        let slice = buf
            .get(..n)
            .ok_or_else(|| anyhow!("short read buffer slice"))?;
        hasher.update(slice);
        if let Some(cb) = progress {
            hashed_bytes = hashed_bytes.saturating_add(u64::try_from(n).unwrap_or(0));
            cb(hashed_bytes);
        }
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Promote a verified `.partial` to the plain `staging` blob (an atomic
/// same-directory rename) and best-effort remove the ranged-store sidecars the
/// dedup path no longer needs. `staging` is then the finalized blob `fetch_group`
/// materializes at each destination — the same shape the whole-file path's
/// ranged store leaves on its own finalize.
fn promote_partial(partial: &Path, staging: &Path) -> anyhow::Result<()> {
    std::fs::rename(partial, staging)
        .with_context(|| format!("promote {} -> {}", partial.display(), staging.display()))?;
    let sidecar = staging.with_extension("partial.ranges");
    if let Err(e) = std::fs::remove_file(&sidecar)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            "failed to remove staging sidecar {}: {e}",
            sidecar.display()
        );
    }
    Ok(())
}

/// Reserved subdirectory of `out_root` holding per-hash staging files and their
/// sidecars. Manifest entry paths that resolve inside it are rejected in
/// [`plan_slots`], so a manifest can never collide with a staging file — nor
/// trick `remove_staging` into deleting a materialized output.
const STAGING_DIR: &str = ".decdn-partial";

/// Per-hash staging file an entry's fetch finalizes to before `fetch_group`
/// materializes it at the manifest's destination path(s) —
/// `<out_root>/.decdn-partial/<hex>` (#1497). This is the *plain* final path:
/// the entry's ranged store owns the `.partial` suffix and the `.ranges`
/// record itself, streaming into `<hex>.partial` beside this and renaming to
/// `<hex>` on `finalize`. Those sidecars are what survives
/// an interrupted pull — a rerun resumes from them rather than re-paying for
/// bytes already landed. Creates the staging directory (idempotent, and only
/// ever called once a group has real work to do — an all-skipped group never
/// creates one) but not the file itself; the ranged store does that.
fn staging_path(out_root: &Path, hash: [u8; 32]) -> anyhow::Result<PathBuf> {
    let dir = out_root.join(STAGING_DIR);
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let name = blake3::Hash::from_bytes(hash).to_hex().to_string();
    Ok(dir.join(name))
}

/// The `(offset, len)` runs of `[0, total)` NOT covered by `donor_aligned`.
///
/// `donor_aligned` holds chunk-group-aligned ranges already available from a
/// donor (a sibling entry's already-pulled bytes) — possibly unsorted and
/// possibly overlapping. This sorts and coalesces them first, then emits the
/// gaps between (and around) the coalesced runs as the complement to drive:
/// the bytes a donor does NOT cover, which still need a paid pull.
///
/// A donor range past `total`, or one that overlaps `total`, is clamped to
/// `total` — `total` is the whole blob's authoritative length. `total == 0`
/// always returns an empty complement (there is nothing to cover).
pub(crate) fn complement_runs(donor_aligned: &[(u64, u64)], total: u64) -> Vec<(u64, u64)> {
    if total == 0 {
        return Vec::new();
    }
    let coalesced = coalesce_runs(donor_aligned, total);

    // Walk the coalesced donor runs, emitting the gap before each one and,
    // at the end, the gap after the last one up to `total`.
    let mut gaps = Vec::with_capacity(coalesced.len() + 1);
    let mut cursor = 0u64;
    for (start, end) in coalesced {
        if start > cursor {
            gaps.push((cursor, start - cursor));
        }
        cursor = cursor.max(end);
    }
    if cursor < total {
        gaps.push((cursor, total - cursor));
    }
    gaps
}

/// Sort and coalesce `(offset, len)` byte ranges into disjoint `(start, end)`
/// runs, clamped to `[0, total)`: ranges that overlap or touch join one run, and
/// an empty or out-of-range one is dropped.
fn coalesce_runs(ranges: &[(u64, u64)], total: u64) -> Vec<(u64, u64)> {
    // Clamp each range to `[0, total)` and drop empty/out-of-range ones, then
    // sort by start so overlapping/adjacent runs coalesce in one pass.
    let mut runs: Vec<(u64, u64)> = ranges
        .iter()
        .filter_map(|&(offset, len)| {
            let start = offset.min(total);
            let end = offset.checked_add(len).unwrap_or(total).min(total);
            (end > start).then_some((start, end))
        })
        .collect();
    runs.sort_unstable_by_key(|&(start, _)| start);

    let mut coalesced: Vec<(u64, u64)> = Vec::with_capacity(runs.len());
    for (start, end) in runs {
        match coalesced.last_mut() {
            Some((_, last_end)) if start <= *last_end => {
                *last_end = (*last_end).max(end);
            }
            _ => coalesced.push((start, end)),
        }
    }
    coalesced
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
/// to resume. Removes the plain `<hex>` finalized blob itself and, best-effort,
/// any leftover `<hex>.partial{,.ranges}` sidecars: the ranged store's
/// `finalize` normally clears those on success, but this is the belt to that
/// brace and never runs on an errored entry (whose sidecars are the resume
/// prefix a rerun needs). A leftover here is harmless clutter, not a
/// correctness issue, so a removal failure is reported and swallowed rather
/// than propagated.
fn remove_staging(staging: &Path) {
    let paths_to_remove = [
        staging.to_path_buf(),
        staging.with_extension("partial"),
        staging.with_extension("partial.ranges"),
    ];
    for path in paths_to_remove {
        if let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!("failed to remove staging file {}: {e}", path.display());
        }
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

impl EntryOutcome {
    fn failed(path: &str, err: &anyhow::Error) -> Self {
        Self::Failed {
            path: path.to_string(),
            // Sanitize here, at the single construction site: a per-entry fetch
            // failure can wrap a chain-RPC error whose source carries the
            // `rpc_url` (API key in path/query). This outcome is both printed to
            // stderr and serialized into the `--json` report, neither of which
            // passes through `main()`'s sanitizer (issue #954).
            err: sanitize_err_chain(err),
        }
    }
}

/// `--node-id` (with its clap-required `--provider-address`) → a pinned target
/// for every entry; otherwise `None` (discover per entry).
fn explicit_target(common: &ClientFetchArgs) -> anyhow::Result<Option<(PublicKey, Address)>> {
    let Some(raw) = &common.node_id else {
        return Ok(None);
    };
    let node_id =
        PublicKey::from_str(raw).map_err(|e| anyhow!("invalid --node-id {raw:?}: {e}"))?;
    let provider_raw = common
        .provider_address
        .as_deref()
        .ok_or_else(|| anyhow!("--provider-address is required with --node-id"))?;
    let provider = chain_ctx::parse_address(provider_raw, "--provider-address")?;
    Ok(Some((node_id, provider)))
}

/// Read + parse a local bundle manifest file.
fn read_local_manifest(path: &Path) -> anyhow::Result<Manifest> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read bundle file {}", path.display()))?;
    parse_manifest(&bytes)
}

/// Parse + version-gate bundle manifest bytes.
fn parse_manifest(bytes: &[u8]) -> anyhow::Result<Manifest> {
    let manifest: Manifest =
        serde_json::from_slice(bytes).context("parse bundle JSON (expected v1 manifest)")?;
    if manifest.version != 1 {
        bail!(
            "unsupported bundle version {} (this build supports v1)",
            manifest.version
        );
    }
    Ok(manifest)
}

/// Resolve a bundle entry's POSIX relative `path` to a filesystem path under
/// `root`, rejecting anything that isn't a sequence of plain filename
/// components — absolute paths, `.`/`..`, drive prefixes, empty components
/// (leading/trailing/double slash). With `..` rejected, the result cannot
/// escape `root`. (`appendix-bundles.md` § Path-safety rules.)
fn safe_join(root: &Path, rel: &str) -> anyhow::Result<PathBuf> {
    if rel.is_empty() {
        bail!("empty entry path");
    }
    let mut out = root.to_path_buf();
    for seg in rel.split('/') {
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

/// The skip-cache record for one entry, built from its FINAL on-disk state, or
/// `None` when this run has nothing safe to record for it. A record is produced
/// only when a regular file is present at the entry's path with a usable mtime —
/// the gates that keep a symlink, a non-regular file, a missing file, or an
/// unreadable mtime out of the later fast-skip path. `materialize` renames new
/// content into place only on success, so a present regular file at the path is
/// this run's landed bytes; pairing the entry's hash with them is sound.
fn record_one(out_root: &Path, en: &ManifestEntry) -> Option<bundle_manifest::SavedFile> {
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
    let chunks = en.chunks.as_ref().map(|cs| {
        cs.iter()
            .map(|c| bundle_manifest::SavedChunk {
                hash: c.hash.clone(),
                size: c.size,
            })
            .collect()
    });
    Some(bundle_manifest::SavedFile {
        hash: en.hash.clone(),
        size: meta.len(),
        mtime,
        chunks,
    })
}

/// One line for each entry whose landed file differs in size from its
/// manifest's `size`: `<path>: manifest says X bytes, the blob is Y bytes`. A
/// manifest size is only the fetch's first claim, so such an entry succeeds;
/// the line tells the user the manifest is stale. `updates` holds the landed
/// files' records ([`build_completed_updates`]).
fn size_warnings(
    entries: &[&ManifestEntry],
    updates: &BTreeMap<String, bundle_manifest::SavedFile>,
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
/// run's outcomes; the result is the same records either way.
fn build_completed_updates(
    entries: &[&ManifestEntry],
    outcomes: &[EntryOutcome],
    out_root: &Path,
) -> BTreeMap<String, bundle_manifest::SavedFile> {
    let mut updates = BTreeMap::new();
    for (en, outcome) in entries.iter().zip(outcomes) {
        let recordable = matches!(
            outcome,
            EntryOutcome::Fetched(_) | EntryOutcome::Linked | EntryOutcome::Skipped
        );
        if recordable && let Some(rec) = record_one(out_root, en) {
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
    updates: BTreeMap<String, bundle_manifest::SavedFile>,
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
        bundle_manifest::merge(&mut acc, batch.updates);
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
    let bytes = match bundle_manifest::serialize(acc) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                "failed to serialize {}: {e}",
                bundle_manifest::SAVED_MANIFEST_NAME
            );
            return false;
        }
    };
    let root = out_root.to_path_buf();
    match tokio::task::spawn_blocking(move || bundle_manifest::write_bytes(&root, &bytes)).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::warn!(
                "failed to write {}: {e}",
                bundle_manifest::SAVED_MANIFEST_NAME
            );
            false
        }
        Err(e) => {
            tracing::warn!("skip-cache flush task panicked: {e}");
            false
        }
    }
}

/// Why a pull ended with no entries to fetch — picks the message
/// [`report_nothing_to_fetch`] prints, so the operator learns the actual cause
/// (an empty bundle, globs that matched nothing, or a fully deselected list)
/// rather than mistaking one for another.
#[derive(Clone, Copy)]
enum NothingReason {
    /// The bundle manifest itself is empty.
    EmptyBundle,
    /// `--include`/`--exclude` removed every entry.
    Filtered,
    /// The user deselected every entry in the `--select` editor.
    Deselected,
}

impl NothingReason {
    /// The reason for an empty result of a filter pass: [`Filtered`] when a filter
    /// was given and the bundle was non-empty before it, else [`EmptyBundle`].
    ///
    /// [`Filtered`]: NothingReason::Filtered
    /// [`EmptyBundle`]: NothingReason::EmptyBundle
    const fn from_filter(filters_given: bool, raw_empty: bool) -> Self {
        if filters_given && !raw_empty {
            Self::Filtered
        } else {
            Self::EmptyBundle
        }
    }
}

fn report_nothing_to_fetch(reason: NothingReason) {
    let msg = match reason {
        NothingReason::EmptyBundle => "bundle has no entries; nothing to fetch",
        NothingReason::Filtered => {
            "no bundle entries match the include/exclude filters; nothing to fetch"
        }
        NothingReason::Deselected => "every file was deselected; nothing to fetch",
    };
    println!("{msg}");
}

/// The manifest a dry run can read with no network: the `-i` file, or the
/// `--hash` bundle's copy in the output root's [`bundle_cache`]. `None` when
/// `--hash` has no usable cached copy (missing, unreadable, or failing its hash
/// check). Errors on a malformed `--hash`, an unreadable `-i` file, or bytes
/// that do not parse as a v1 manifest. The cache is read even under
/// `--overwrite`, which makes a real pull fetch the manifest again: the cache
/// is content-addressed, so its bytes are exactly what that fetch returns.
fn dry_run_manifest(args: &BundlePullArgs) -> anyhow::Result<Option<Manifest>> {
    match (&args.input, &args.hash) {
        (Some(path), _) => read_local_manifest(path).map(Some),
        (None, Some(h)) => {
            let hash = fetch::parse_hash(h)?;
            bundle_cache::load(&args.output, hash)
                .map(|bytes| {
                    parse_manifest(&bytes).with_context(|| {
                        format!(
                            "cached bundle manifest {}",
                            bundle_cache::cache_path(&args.output, hash).display()
                        )
                    })
                })
                .transpose()
        }
        (None, None) => bail!("no bundle source (expected -i or --hash)"),
    }
}

/// Print the would-fetch plan and exit (no network/chain/keystore activity).
/// The plan lists the entries of the manifest [`dry_run_manifest`] reads: the
/// `-i` file, or a `--hash` bundle's cached manifest. The `--include`/`--exclude`
/// `filter` applies to those entries, so the plan is the entry set a real run
/// starts from and the unmatched-pattern warnings are the ones it prints. A real
/// run then skips files already present in the output root (unless
/// `--overwrite`) and warns on leftover partials; the dry run does neither. A
/// `--hash` bundle with no cached manifest reports only the bundle and the
/// output root, with `entries: null` under `--json`.
fn dry_run(args: &BundlePullArgs, filter: &EntryFilter, filters_given: bool) -> anyhow::Result<()> {
    let out = args.output.display();
    let Some(mut manifest) = dry_run_manifest(args)? else {
        let h = args.hash.as_deref().unwrap_or_default();
        if args.json {
            let plan = serde_json::json!({
                "output": out.to_string(),
                "hash": h,
                "count": null,
                "entries": null,
            });
            println!("{plan}");
            return Ok(());
        }
        let filter_note = if filters_given {
            "; the include/exclude patterns are matched against them then"
        } else {
            ""
        };
        println!(
            "--dry-run with --hash: would fetch bundle {h} then its entries into {out} \
             (no cached manifest: entries are listed once a pull into this directory \
             caches it{filter_note})"
        );
        return Ok(());
    };
    let raw_empty = manifest.entries.is_empty();
    manifest.entries = filter.apply_and_warn(manifest.entries).0;
    if args.json {
        // The `--json` plan stays machine-readable — an empty set is
        // `count: 0` with an empty `entries` array, no prose line. A `--hash`
        // bundle with no cached manifest gives `count`/`entries` of `null`.
        let plan = serde_json::json!({
            "output": args.output.display().to_string(),
            "count": manifest.entries.len(),
            "entries": manifest.entries.iter().map(|e| serde_json::json!({
                "path": e.path, "hash": e.hash, "size": e.size,
                "chunks": e.chunks.as_ref().map(Vec::len),
            })).collect::<Vec<_>>(),
        });
        println!("{plan}");
    } else if manifest.entries.is_empty() {
        // Match the real run's empty-result message rather than printing a
        // "would fetch 0 entr(ies)" plan, so `--dry-run` and a live pull
        // agree on what an emptied set looks like.
        report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
    } else {
        println!(
            "would fetch {} entr(ies) into {out}:",
            manifest.entries.len()
        );
        for e in &manifest.entries {
            let chunks = e
                .chunks
                .as_ref()
                .map(|c| format!(", {} chunks", c.len()))
                .unwrap_or_default();
            match e.size {
                Some(n) => println!("  {} ({n} bytes{chunks})", e.path),
                None => println!("  {}{}", e.path, chunks),
            }
        }
    }
    Ok(())
}

/// Summarize outcomes; return an error if any entry failed (after reporting all).
/// Each of `warnings` ([`size_warnings`]) prints one line on stderr and never
/// changes the result. `excluded` is the count of entries the run does not pull
/// ([`PullReport`]).
fn report(
    outcomes: &[EntryOutcome],
    warnings: &[String],
    transfer: Transfer,
    dedup: DedupSummary,
    excluded: u64,
    output: &Path,
    json: bool,
) -> anyhow::Result<()> {
    for o in outcomes {
        if let EntryOutcome::Failed { path, err } = o {
            // A per-entry failure is a command result the user needs, not
            // routing narration: keep it on stderr (unconditional, and clear
            // of the `--json` report on stdout) rather than behind logging.
            eprintln!("failed: {path}: {err}");
        }
    }
    for line in warnings {
        eprintln!("warning: {line}");
    }

    let rep = pull_report(outcomes, transfer, dedup, excluded, output);
    let (reused, reused_bytes, failed) = (rep.reused, rep.reused_bytes, rep.failed);
    if json {
        let line = serde_json::to_string(&rep).map_err(|e| anyhow!("serialize report: {e}"))?;
        println!("{line}");
    } else {
        println!("{}", counts_line(&rep));
        // `downloaded X → reconstructed Y` only when dedup made them differ;
        // otherwise a single `downloaded X`.
        println!("{}", transfer_line(transfer));
        // The bytes earlier, interrupted runs fetched and this run resumed, shown
        // only when a run resumed any: they are in neither total above.
        if transfer.resumed > 0 {
            println!(
                "resumed {} from earlier partials",
                human_bytes(transfer.resumed)
            );
        }
        // The whole-file dedup outcome, shown only when it mattered: a run that
        // materialized any destination from an on-disk donor instead of fetching.
        if reused > 0 {
            println!(
                "whole-file dedup: reused {} from disk ({reused} file(s))",
                human_bytes(reused_bytes)
            );
        }
        // The range-dedup outcome, shown only when a hint mattered: a run that
        // spliced bytes from disk, or one whose hints were dropped by a fault. A
        // plain pull with no usable hints stays silent (both are zero), so a
        // hint-carrying bundle whose hints all lie (spliced 0, some ignored) no
        // longer prints the same summary as an unhinted one.
        if dedup.spliced_bytes > 0 || dedup.hints_ignored > 0 {
            println!(
                "range-dedup: spliced {} from disk, {} hint(s) ignored",
                human_bytes(dedup.spliced_bytes),
                dedup.hints_ignored
            );
        }
    }

    if failed > 0 {
        bail!("{failed} entr(ies) failed to fetch");
    }
    Ok(())
}

/// The run's [`PullReport`]: the entry counts tallied from `outcomes`, beside
/// the byte tallies and the `excluded` count the caller already holds.
fn pull_report(
    outcomes: &[EntryOutcome],
    transfer: Transfer,
    dedup: DedupSummary,
    excluded: u64,
    output: &Path,
) -> PullReport {
    let mut rep = PullReport {
        output: output.display().to_string(),
        fetched: 0,
        linked: 0,
        skipped: 0,
        excluded,
        failed: 0,
        downloaded: transfer.downloaded,
        reconstructed: transfer.reconstructed,
        spliced_bytes: dedup.spliced_bytes,
        hints_ignored: dedup.hints_ignored,
        resumed_bytes: transfer.resumed,
        reused: 0,
        reused_bytes: 0,
    };
    for o in outcomes {
        match o {
            EntryOutcome::Fetched(_) => rep.fetched += 1,
            EntryOutcome::Linked => rep.linked += 1,
            EntryOutcome::Skipped => rep.skipped += 1,
            EntryOutcome::Deduped(n) => {
                rep.reused += 1;
                rep.reused_bytes = rep.reused_bytes.saturating_add(*n);
            }
            EntryOutcome::Failed { .. } => rep.failed += 1,
        }
    }
    rep
}

/// The summary's first line: where the run pulled to, and every entry count.
fn counts_line(rep: &PullReport) -> String {
    format!(
        "pulled into {} ({} fetched, {} linked, {} skipped, {} reused, {} excluded, {} failed)",
        rep.output, rep.fetched, rep.linked, rep.skipped, rep.reused, rep.excluded, rep.failed
    )
}

/// Scan `<out_root>/.decdn-partial` for leftover per-hash staging files —
/// `<hex>`, `<hex>.partial`, `<hex>.partial.ranges` — whose hash is not in
/// `keep`, and total the disk space they take ([`allocated_bytes`]). Other
/// names (a record's temporary file) are not counted. A missing or unreadable
/// directory holds nothing.
fn leftover_partials(out_root: &Path, keep: &HashSet<[u8; 32]>) -> Leftovers {
    let Ok(dir) = std::fs::read_dir(out_root.join(STAGING_DIR)) else {
        return Leftovers::default();
    };
    let mut blobs: HashSet<[u8; 32]> = HashSet::new();
    let mut bytes = 0u64;
    for entry in dir.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let stem = name
            .strip_suffix(".partial.ranges")
            .or_else(|| name.strip_suffix(".partial"))
            .unwrap_or(name);
        let Ok(hash) = blake3::Hash::from_hex(stem) else {
            continue;
        };
        let hash = *hash.as_bytes();
        if keep.contains(&hash) {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_file() {
            blobs.insert(hash);
            bytes = bytes.saturating_add(allocated_bytes(&meta));
        }
    }
    Leftovers {
        blobs: u64::try_from(blobs.len()).unwrap_or(u64::MAX),
        bytes,
    }
}

/// The disk space a file takes. A range-dedup `.partial` is sized to the whole
/// blob before any byte lands, so its length can be far above the space that
/// deleting it frees. On Unix the allocated blocks give that space; elsewhere
/// the length is the best figure.
fn allocated_bytes(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}

/// Staging files in the staging directory that a run did not use: how many
/// distinct blobs, and the disk space their files take.
#[derive(Debug, Default, PartialEq, Eq)]
struct Leftovers {
    blobs: u64,
    bytes: u64,
}

/// The warning line for `leftovers` in `dir`, without the `warning: ` prefix,
/// or `None` when there are none.
fn leftover_warning(dir: &Path, leftovers: &Leftovers) -> Option<String> {
    let size = human_bytes(leftovers.bytes);
    let dir = dir.display();
    match leftovers.blobs {
        0 => None,
        1 => Some(format!(
            "1 partial download ({size} on disk) that this run did not use remains in \
             {dir}; delete it to reclaim the space"
        )),
        n => Some(format!(
            "{n} partial downloads ({size} on disk) that this run did not use remain in \
             {dir}; delete them to reclaim the space"
        )),
    }
}

/// Warn on stderr about leftover staging files of blobs this run did not use
/// ([`leftover_partials`]): a partial of an entry an earlier run started,
/// which this run's filter or selection left out, or one another bundle
/// pulled into the same output directory left. They stay for a later run that
/// selects the entry again, so the warning only names them and never deletes
/// them. `entries` are the run's selected entries (none when the filter or
/// `--select` emptied the run), and `bundle_hash` is the `--hash` manifest
/// blob, which is also this run's own.
fn warn_leftover_partials(out_root: &Path, entries: &[ManifestEntry], bundle_hash: Option<&str>) {
    let keep: HashSet<[u8; 32]> = entries
        .iter()
        .map(|e| e.hash.as_str())
        .chain(bundle_hash)
        .filter_map(|h| fetch::parse_hash(h).ok())
        .collect();
    let leftovers = leftover_partials(out_root, &keep);
    if let Some(line) = leftover_warning(&out_root.join(STAGING_DIR), &leftovers) {
        eprintln!("warning: {line}");
    }
}

/// Format a byte count as a short decimal-unit label (`13.8 GB`). SI (1000-based)
/// units match how file and model sizes are usually quoted.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if n < 1000 {
        return format!("{n} B");
    }
    // Precision loss is irrelevant here: the result is a one-decimal human label.
    #[allow(
        clippy::cast_precision_loss,
        reason = "display-only size label; exact integer value is not needed"
    )]
    let mut value = n as f64;
    let mut unit = 0usize;
    while value >= 1000.0 && unit + 1 < UNITS.len() {
        value /= 1000.0;
        unit += 1;
    }
    let label = UNITS.get(unit).copied().unwrap_or("B");
    format!("{value:.1} {label}")
}

/// The end-of-pull transfer line. When dedup saved a transfer the two totals
/// differ and both are shown with an arrow; otherwise a single `downloaded X`
/// (the `→ reconstructed` half is omitted rather than repeating the same figure).
fn transfer_line(t: Transfer) -> String {
    if t.downloaded == t.reconstructed {
        format!("downloaded {}", human_bytes(t.downloaded))
    } else {
        format!(
            "downloaded {} → reconstructed {}",
            human_bytes(t.downloaded),
            human_bytes(t.reconstructed)
        )
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
