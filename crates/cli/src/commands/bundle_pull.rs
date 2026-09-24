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
//! bounded by one global `--jobs` cap (`PullCtx.gate`). A manifest `chunks`
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};

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
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use serde::{Deserialize, Serialize};

use super::bundle_cache;
use super::bundle_manifest::{self, SavedManifest, SavedMtime};
use super::buyer_store::{ChainAdoption, open_client_store_for_buy};
use super::chain_ctx;
use super::fetch;
use super::manifest::build_glob_set;
use super::pull_progress::{self, PullProgress};
use decdn_bao_range::CHUNK_GROUP_BYTES;
use decdn_client::discovery::{self, NodeCandidate};
use decdn_client::endpoint as client_endpoint;
use decdn_client::provider;
use decdn_client::{
    ClientRangedStore, LaneLedgers, PoolContext, PoolExhausted, ProgressCallback, PullDeadlines,
    RetryDisposition, retry_disposition, shared_pool_disposition,
};

type FetchTarget = (PublicKey, Address);

/// A dedup entry's resolved range-drive provider order, computed once per entry
/// and reused across its complement drive, donor re-fetch, and any whole-blob
/// re-drive (so a self-heal entry probes once, not per sub-drive). `Pinned` is
/// the `--node-id` target — its own only candidate; `Discovered` is the probed
/// discovery order.
enum RangeTargets {
    Pinned(FetchTarget),
    /// The probed order. When `stripe` is two or more, its first `stripe`
    /// candidates are the admitted full holders the entry's drives stripe
    /// across ([`stripe_order`]) and the rest are failover reserve. When it is
    /// one, the order is the plain failover order, and its first candidate
    /// need not be a full holder.
    Discovered {
        order: Vec<NodeCandidate>,
        stripe: usize,
    },
}

impl RangeTargets {
    /// The distinct provider addresses this entry's range drives may stream from —
    /// the pinned target's one provider, or every discovered candidate's. Used to
    /// acquire the entry's lane-stream permit set before its drives.
    fn providers(&self) -> Vec<Address> {
        match self {
            RangeTargets::Pinned((_, provider)) => vec![*provider],
            RangeTargets::Discovered { order, .. } => order.iter().map(|c| c.eth_address).collect(),
        }
    }

    /// How many of the first candidates a drive stripes across at once.
    const fn stripe(&self) -> usize {
        match self {
            RangeTargets::Pinned(_) => 1,
            RangeTargets::Discovered { stripe, .. } => *stripe,
        }
    }

    /// Each candidate's dial target and registry dial hints, in failover order.
    /// The pinned target has no hints: it is reached through `--addr`.
    fn targets(&self) -> Vec<(FetchTarget, Vec<std::net::SocketAddr>)> {
        match self {
            RangeTargets::Pinned(pinned) => vec![(*pinned, Vec::new())],
            RangeTargets::Discovered { order, .. } => order
                .iter()
                .map(|c| ((c.node_id, c.eth_address), c.dial_addrs()))
                .collect(),
        }
    }
}

/// A range-dedup entry's candidate order with its stripe set first (#2123),
/// and the size of that set.
///
/// The stripe set is the probed holders whose coverage spans the whole blob,
/// admitted as the multi-source fan-out admits its lanes
/// ([`discovery::admit_sources`]): one node per operator, at most
/// `max_sources`. A partial holder or a proxy-warming non-holder is left out,
/// so no striped range lands on a node that lacks it. The rest of `candidates`
/// follow in their own order as failover reserve. With multi-source off, or
/// fewer than two admitted holders, the order is unchanged and the stripe is
/// one: plain single-source failover.
fn stripe_order(
    candidates: Vec<NodeCandidate>,
    coverage_by_node: &HashMap<PublicKey, decdn_protocol::Coverage>,
    total: u64,
    multi_source: bool,
    max_sources: usize,
) -> (Vec<NodeCandidate>, usize) {
    if !multi_source {
        return (candidates, 1);
    }
    let blocks = decdn_protocol::num_blocks(total);
    let full_holders: Vec<NodeCandidate> = candidates
        .iter()
        .filter(|c| {
            coverage_by_node
                .get(&c.node_id)
                .is_some_and(|cov| (0..blocks).all(|b| cov.covers(b)))
        })
        .cloned()
        .collect();
    let admitted = discovery::admit_sources(full_holders, max_sources);
    if admitted.len() < 2 {
        return (candidates, 1);
    }
    let stripe = admitted.len();
    let striped: HashSet<PublicKey> = admitted.iter().map(|a| a.node_id).collect();
    let reserve = candidates
        .into_iter()
        .filter(|c| !striped.contains(&c.node_id));
    (admitted.into_iter().chain(reserve).collect(), stripe)
}

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
/// a serving node then ends them at its voucher-read timeout. `what` names the
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
        // Fast-skip: saved record agrees on hash, size, and mtime.
        let fast = saved.get(&en.path).is_some_and(|rec| {
            rec.hash == en.hash
                && rec.size == meta.len()
                && en.size.is_none_or(|s| s == rec.size)
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
#[derive(Debug)]
struct EntryFilter {
    include: globset::GlobSet,
    /// Whether any `--include` was given. False leaves the include gate open —
    /// distinct from an empty [`globset::GlobSet`], which matches nothing.
    has_include: bool,
    exclude: globset::GlobSet,
}

impl EntryFilter {
    /// Compile the run's `--include`/`--exclude` patterns. A malformed glob is a
    /// hard error naming the flag it came from.
    fn compile(include: &[String], exclude: &[String]) -> anyhow::Result<Self> {
        Ok(Self {
            include: build_glob_set(include, "--include")?,
            has_include: !include.is_empty(),
            exclude: build_glob_set(exclude, "--exclude")?,
        })
    }

    /// Whether an entry at POSIX relative `path` survives the filter.
    fn keep(&self, path: &str) -> bool {
        let p = Path::new(path);
        (!self.has_include || self.include.is_match(p)) && !self.exclude.is_match(p)
    }

    /// Retain only the entries the filter keeps, preserving manifest order.
    fn apply(&self, entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
        entries.into_iter().filter(|e| self.keep(&e.path)).collect()
    }
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
/// through unchanged when `--select` is off. `Ok(None)` means the user deselected
/// every file: [`report_nothing_to_fetch`] was already called, so the run is done.
fn maybe_select(args: &BundlePullArgs, mut manifest: Manifest) -> anyhow::Result<Option<Manifest>> {
    if args.select {
        manifest.entries = select_entries(manifest.entries)?;
        if manifest.entries.is_empty() {
            report_nothing_to_fetch(NothingReason::Deselected);
            return Ok(None);
        }
    }
    Ok(Some(manifest))
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

/// One hash-group's result for one round of the pull.
struct GroupRun {
    /// One outcome per entry in the group, in order.
    outcomes: Vec<EntryOutcome>,
    /// The content bytes this round paid for, when its blob fetch landed
    /// ([`PullCtx::pull_entry_untimed`]). `None` when the round fetched nothing.
    paid: Option<u64>,
    /// The group's fetch failed with an error a later `--entry-retries` round
    /// may cure ([`entry_retryable`]). Only a failed blob fetch sets it; a
    /// materialize failure keeps its paid staging blob for the next run instead.
    retry: bool,
}

impl GroupRun {
    /// A result no later round can change.
    const fn done(outcomes: Vec<EntryOutcome>) -> Self {
        Self {
            outcomes,
            paid: None,
            retry: false,
        }
    }

    /// The group's blob fetch landed, paying for `paid` content bytes, and
    /// `outcomes` is how each destination then materialized. No later round can
    /// change it.
    const fn landed(outcomes: Vec<EntryOutcome>, paid: u64) -> Self {
        Self {
            outcomes,
            paid: Some(paid),
            retry: false,
        }
    }

    /// The group's blob fetch failed with `err`: every writable slot fails, and
    /// the group goes into the next round when [`entry_retryable`] says a round
    /// can fix it.
    fn fetch_failed(slots: Vec<Slot<'_>>, err: &anyhow::Error) -> Self {
        Self {
            retry: entry_retryable(err),
            outcomes: fail_all(slots, err),
            paid: None,
        }
    }
}

/// One-line `--json` summary. `fetched`/`linked`/`skipped`/`failed` are entry
/// counts; `downloaded` is the content bytes paid for across the distinct blobs
/// fetched (a blob shared across several paths counts once, #1306; a range-dedup
/// blob counts only the bytes it did not splice from disk) and `reconstructed` is
/// the total bytes written to disk this run — they diverge when one blob is
/// materialized to several paths or when range-dedup spliced part of a blob.
/// `downloaded` is a content-size tally, not an exact on-wire measurement: it
/// excludes bao proof overhead, and it counts a `.partial` prefix resumed from an
/// earlier run.
///
/// `deduped` and `reused_bytes` report the whole-file dedup outcome, counted per
/// distinct blob exactly as `fetched`/`downloaded` are (a blob reused at several
/// paths counts once here; its extra destinations are `linked`): `deduped` is the
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
    failed: u64,
    downloaded: u64,
    reconstructed: u64,
    spliced_bytes: u64,
    hints_ignored: u64,
    /// Count of distinct blobs materialized from an on-disk whole-file donor;
    /// extra destinations of the same blob are counted in `linked`.
    deduped: u64,
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

/// One dedup entry's range-dedup outcome, returned by [`reassemble_dedup`] and
/// accumulated into [`PullCtx::dedup_stats`] by [`PullCtx::pull_entry`].
#[derive(Clone, Copy, Default, Debug)]
struct DedupOutcome {
    /// Bytes this entry served from a local donor splice — never downloaded.
    spliced_bytes: u64,
    /// Range-dedup hints this entry dropped by a fault (a donor that failed its
    /// verification re-hash or was unreadable, or a self-heal re-drive that
    /// discarded every already-spliced donor).
    hints_ignored: u64,
}

/// Run-scoped range-dedup counters, shared by every concurrent entry via
/// `&PullCtx`. Atomics because entries run concurrently under `buffer_unordered`;
/// `Relaxed` is enough — the totals are read once, after the pull joins.
#[derive(Default)]
struct DedupStats {
    spliced_bytes: AtomicU64,
    hints_ignored: AtomicU64,
}

/// A pull's byte accounting: `downloaded` is the content bytes paid for across the
/// distinct blobs fetched (a blob materialized to several paths counts once, #1306;
/// a range-dedup blob counts only the bytes it did not splice from disk);
/// `reconstructed` is the total bytes written to disk (every materialized copy).
/// `downloaded` is content bytes, not exact on-wire bytes: it omits bao proof
/// overhead and still counts a `.partial` prefix resumed from an earlier run.
#[derive(Clone, Copy, Default)]
struct Transfer {
    downloaded: u64,
    reconstructed: u64,
}

impl Transfer {
    /// Combine two tallies (saturating — a pull never reports a wrapped total).
    const fn add(self, other: Transfer) -> Transfer {
        Transfer {
            downloaded: self.downloaded.saturating_add(other.downloaded),
            reconstructed: self.reconstructed.saturating_add(other.reconstructed),
        }
    }
}

/// Read the registry once and keep the region-nearest candidates, which every
/// entry in the manifest then reuses.
async fn discover_candidates(
    chain: &fetch::ResolvedChain,
    registry_cap: std::time::Duration,
) -> anyhow::Result<Vec<discovery::NodeCandidate>> {
    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow!(
            "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
             blockchain.capacity_bond_address), or pass --node-id to pull from one node"
        )
    })?;
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
    Ok(discovery::select_candidates(
        all,
        chain.region.as_deref(),
        discovery::SELECT_K,
    ))
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
    /// entry against `candidates`.
    explicit: Option<FetchTarget>,
    candidates: Option<Vec<NodeCandidate>>,
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
    // Explicit single node for every entry, or a per-entry discovery candidate list
    // read from `CapacityBond` once.
    let explicit = explicit_target(common)?;
    let candidates = match explicit {
        Some(_) => None,
        // The registry read is bounded by `--timeout-ms` (#1349), inside
        // `bootstrap_nodes` so a timeout still falls through to the peer store.
        // No probing happens here — `discover_candidates` is the registry read
        // plus `select_candidates`; probing is per entry, in `pick_excluding`
        // below.
        None => Some(discover_candidates(chain, common.discovery_cap()).await?),
    };
    let (signer, self_address) = load_buyer_signer(chain)?;
    Ok(Selection {
        explicit,
        candidates,
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
            m.entries = filter.apply(m.entries);
            if m.entries.is_empty() {
                report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
                return Ok(());
            }
            Some(m)
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
    let store = open_client_store_for_buy(&chain.data_dir, chain.data_dir_source, "pull")?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    // The body runs in `pull_over`, so the endpoint closes on every exit —
    // success, an early return, or an error — and its open connections end
    // cleanly instead of being aborted on drop.
    let result = pull_over(
        args,
        &chain,
        grant,
        &store,
        &endpoint,
        &relays,
        (&filter, filters_given),
        local_manifest,
    )
    .await;
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
    store: &RedbBuyerPoolStore,
    endpoint: &Endpoint,
    relays: &[RelayUrl],
    (filter, filters_given): (&EntryFilter, bool),
    local_manifest: Option<Manifest>,
) -> anyhow::Result<()> {
    let common = &args.common;
    // Selection + the buyer signer, resolved per path (see `resolve_selection`).
    let Selection {
        explicit,
        candidates,
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
        candidates,
        common,
        namespace_id,
        grant,
        ledgers: LaneLedgers::new(),
        dedup_stats: DedupStats::default(),
        open_lock: tokio::sync::Mutex::new(()),
        jobs: args.jobs.max(1),
        gate: tokio::sync::Semaphore::new(args.jobs.max(1)),
        lane_cap: LaneStreamCap::new(args.max_lane_streams),
        entry_retries: args.entry_retries,
        // Silent during the manifest fetch below (a single blob); replaced once
        // the kept entries are known and their sizes decide the total-bar mode.
        progress: PullProgress::disabled(),
    };

    pull_manifest(ctx, args, filter, filters_given, local_manifest).await
}

/// Obtain the bundle manifest through `ctx`, pull every kept entry, and print the
/// run summary.
async fn pull_manifest<P: Provider + Clone>(
    mut ctx: PullCtx<'_, P>,
    args: &BundlePullArgs,
    filter: &EntryFilter,
    filters_given: bool,
    local_manifest: Option<Manifest>,
) -> anyhow::Result<()> {
    // Obtain the manifest: the pre-read local one, or the `--hash` bundle blob
    // fetched and filtered here. `None` => filtered to empty (already reported).
    let Some(manifest) = obtain_manifest(&ctx, args, filter, filters_given, local_manifest).await?
    else {
        return Ok(());
    };

    // `--select`: let the user trim the (already glob-filtered) list in their
    // editor. Everything deselected ends the run (reported) like an empty filter.
    let Some(manifest) = maybe_select(args, manifest)? else {
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
    let (outcomes, transfer) = ctx
        .pull_all(&manifest.entries, &args.output, args.overwrite, saved)
        .await;
    ctx.progress.finish();

    // Every entry has joined, so the shared dedup counters are now stable.
    let dedup = DedupSummary {
        spliced_bytes: ctx.dedup_stats.spliced_bytes.load(Ordering::Relaxed),
        hints_ignored: ctx.dedup_stats.hints_ignored.load(Ordering::Relaxed),
    };
    report(&outcomes, transfer, dedup, &args.output, args.json)
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
    local_manifest: Option<Manifest>,
) -> anyhow::Result<Option<Manifest>> {
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
    m.entries = filter.apply(m.entries);
    if m.entries.is_empty() {
        report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
        return Ok(None);
    }
    Ok(Some(m))
}

/// Record one group's run in [`PullCtx::pull_plain`]: forward its recordable
/// files to the skip-cache flush, put its outcomes in slot `i` (replacing an
/// earlier round's), and return whether it goes into the next retry round.
/// Each failed entry that does is logged with its error, so a first-round
/// failure is visible before a later round replaces it.
fn settle_group_run(
    groups: &mut [SettledGroup],
    flush_tx: &tokio::sync::mpsc::UnboundedSender<FlushBatch>,
    i: usize,
    run: GroupRun,
    updates: BTreeMap<String, bundle_manifest::SavedFile>,
) -> bool {
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
    if run.retry {
        for outcome in &run.outcomes {
            if let EntryOutcome::Failed { path, err } = outcome {
                tracing::warn!(
                    "bundle pull: {path} failed ({err}); it goes into the next retry round"
                );
            }
        }
    }
    if let Some(slot) = groups.get_mut(i) {
        *slot = SettledGroup {
            outcomes: run.outcomes,
            paid: run.paid,
        };
    }
    run.retry
}

/// A hash-group's latest-round result, kept for the run summary: its per-entry
/// outcomes and the content bytes its landed fetch paid for (see
/// [`GroupRun::paid`]).
#[derive(Clone, Default)]
struct SettledGroup {
    outcomes: Vec<EntryOutcome>,
    paid: Option<u64>,
}

/// Walk one entry's ordered candidates with single-source failover (#1174,
/// ADR 037 § Fallback): try each in turn, stop on the first success or a
/// [`RetryDisposition::Terminal`] error, and fail over on anything else.
///
/// A shared-pool exhaustion ([`PoolExhausted`]) fails over too, per
/// [`retry_disposition`]: another provider's rate may fit what the deposit
/// holds. Every failover logs a warning naming the provider, and so does the
/// last one. The error that exhausts the list carries the candidate count as
/// context, so the end-of-run summary says the entry ran out of providers and
/// not only what the last one said (#2118). The context wraps the original
/// error, so [`retry_disposition`] still classifies it.
async fn walk_candidates<'c, C, T, Fut>(
    order: &'c [C],
    what: &str,
    label: impl Fn(&C) -> String,
    mut attempt: impl FnMut(&'c C) -> Fut,
) -> anyhow::Result<T>
where
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let n = order.len();
    for (i, cand) in order.iter().enumerate() {
        let err = match attempt(cand).await {
            Ok(v) => return Ok(v),
            Err(err) => err,
        };
        if retry_disposition(&err) == RetryDisposition::Terminal {
            return Err(err);
        }
        if i + 1 < n {
            tracing::warn!(
                "bundle pull: provider {} could not deliver {what} ({err:#}); failing over to \
                 the next of {n} candidate(s)",
                label(cand),
            );
            continue;
        }
        tracing::warn!(
            "bundle pull: provider {} could not deliver {what} ({err:#}); every one of {n} \
             candidate(s) has now failed",
            label(cand),
        );
        return Err(err.context(format!(
            "all {n} candidate provider(s) failed to deliver {what}"
        )));
    }
    Err(anyhow!("no candidate node could deliver {what}"))
}

/// Whether a failed entry is worth another round after the first pass
/// (`--entry-retries`, #2118).
///
/// A [`RetryDisposition::RetryElsewhere`] failure is a property of the
/// providers the entry tried, so a later round, which probes again and
/// re-admits every provider, can succeed. A [`PoolExhausted`] is excluded even
/// though it fails over within a pass ([`shared_pool_disposition`]): every
/// provider already refused the deposit, and another round would only repeat
/// the refusal.
///
/// Two more failures fail over within a pass but never start a round: a size
/// that disagrees with the manifest ([`fetch::ManifestSizeMismatch`] at a
/// primed first open, [`decdn_client::SignedSizeMismatch`] at any other
/// leg), which every honest provider repeats, and a local disk fault (no permission, a full
/// or read-only disk, a path that is not a directory), which no provider can
/// fix.
fn entry_retryable(err: &anyhow::Error) -> bool {
    shared_pool_disposition(err) == RetryDisposition::RetryElsewhere
        && err.downcast_ref::<fetch::ManifestSizeMismatch>().is_none()
        && err
            .downcast_ref::<decdn_client::SignedSizeMismatch>()
            .is_none()
        && !is_local_disk_fault(err)
}

/// Whether `err`'s chain holds an I/O error only this machine can fix.
fn is_local_disk_fault(err: &anyhow::Error) -> bool {
    use std::io::ErrorKind;
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
        .any(|io| {
            matches!(
                io.kind(),
                ErrorKind::PermissionDenied
                    | ErrorKind::StorageFull
                    | ErrorKind::ReadOnlyFilesystem
                    | ErrorKind::NotADirectory
                    | ErrorKind::IsADirectory
                    | ErrorKind::FileTooLarge
            )
        })
}

/// The wait before retry round `round` (1-based): 2 s, doubling, capped at
/// 30 s. The wait gives a provider that dropped out time to recover before
/// the round probes it again.
fn entry_retry_backoff(round: u32) -> std::time::Duration {
    const BASE: std::time::Duration = std::time::Duration::from_secs(2);
    const CAP: std::time::Duration = std::time::Duration::from_secs(30);
    BASE.saturating_mul(2u32.saturating_pow(round.saturating_sub(1)))
        .min(CAP)
}

/// Run `items` through `run`, at most `jobs` at a time, then re-run the items
/// whose result asks for it for up to `retries` more rounds, waiting
/// [`entry_retry_backoff`] before each round.
///
/// `settle` sees every result as it lands, with the item's index in `items`,
/// and returns whether that item should go into the next round. A later
/// round's result for an item replaces the earlier one in whatever `settle`
/// records.
async fn run_with_retries<G, R, Fut>(
    items: Vec<G>,
    jobs: usize,
    retries: u32,
    run: impl Fn(G) -> Fut,
    mut settle: impl FnMut(usize, R) -> bool,
) where
    G: Clone,
    Fut: std::future::Future<Output = R>,
{
    let mut pending: Vec<(usize, G)> = items.into_iter().enumerate().collect();
    let mut round = 0u32;
    while !pending.is_empty() {
        let mut next: Vec<(usize, G)> = Vec::new();
        let width = jobs.min(pending.len()).max(1);
        let mut stream = futures_util::stream::iter(pending)
            .map(|(i, item)| {
                let fut = run(item.clone());
                async move { (i, item, fut.await) }
            })
            .buffer_unordered(width);
        while let Some((i, item, result)) = stream.next().await {
            if settle(i, result) {
                next.push((i, item));
            }
        }
        drop(stream);
        if next.is_empty() || round >= retries {
            return;
        }
        round += 1;
        let wait = entry_retry_backoff(round);
        tracing::warn!(
            "bundle pull: {} entr(ies) failed with a retryable error; retry round {round} of \
             {retries} starts in {}s",
            next.len(),
            wait.as_secs(),
        );
        tokio::time::sleep(wait).await;
        pending = next;
    }
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
struct LaneStreamCap {
    /// Per-provider semaphores, created on first use. The `tokio::sync::Mutex`
    /// guards the map so the cap is `Sync` and shareable across the entry futures.
    map: tokio::sync::Mutex<HashMap<Address, Arc<tokio::sync::Semaphore>>>,
    /// Concurrent-stream permits per provider (at least 1). At 1 a `Semaphore(1)`
    /// serializes same-lane streams exactly like a mutex.
    n: usize,
}

impl LaneStreamCap {
    /// A cap admitting `n` concurrent streams per provider (clamped to at least 1).
    fn new(n: usize) -> Self {
        Self {
            map: tokio::sync::Mutex::new(HashMap::new()),
            n: n.max(1),
        }
    }

    /// The per-provider semaphore, created on first use.
    async fn semaphore(&self, provider: Address) -> Arc<tokio::sync::Semaphore> {
        let mut map = self.map.lock().await;
        Arc::clone(
            map.entry(provider)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(self.n))),
        )
    }

    /// Acquire one stream permit for `provider`, held until the returned permit
    /// drops. At `n == 1` a second concurrent caller for the same provider waits
    /// here until the first releases.
    async fn permit(&self, provider: Address) -> anyhow::Result<tokio::sync::OwnedSemaphorePermit> {
        self.semaphore(provider)
            .await
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("bundle pull lane-stream cap closed"))
    }

    /// Up to `max` more stream permits for `provider`, taking only those free
    /// right now. Never waits, so it cannot deadlock against a caller that holds
    /// one permit set and waits for another; an entry that gets none drives with
    /// the one permit it already holds.
    async fn try_extra(
        &self,
        provider: Address,
        max: usize,
    ) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let sem = self.semaphore(provider).await;
        std::iter::from_fn(|| Arc::clone(&sem).try_acquire_owned().ok())
            .take(max)
            .collect()
    }

    /// Acquire one stream permit for every distinct provider in `providers`, in
    /// one global order (sorted, deduped `Address`), and return them held for the
    /// caller's whole fetch. Acquiring every multi-provider set in the same order
    /// makes the cap deadlock-free: a task never waits on a lower-address permit
    /// while holding a higher one.
    async fn permit_set(
        &self,
        providers: &[Address],
    ) -> anyhow::Result<Vec<tokio::sync::OwnedSemaphorePermit>> {
        let mut ordered = providers.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        let mut permits = Vec::with_capacity(ordered.len());
        for provider in ordered {
            permits.push(self.permit(provider).await?);
        }
        Ok(permits)
    }
}

/// Shared, by-reference state for the entry fetch loop. Borrowed by every
/// in-flight entry future. `LaneLedgers` is `Sync`, so `PullCtx` is `Sync` and
/// safe to share across `tokio::spawn` if needed; `buffer_unordered` currently
/// polls in one task.
struct PullCtx<'a, P: Provider + Clone> {
    endpoint: &'a Endpoint,
    store: &'a RedbBuyerPoolStore,
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
    /// `None` discovers per entry against `candidates`.
    explicit: Option<FetchTarget>,
    candidates: Option<Vec<NodeCandidate>>,
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
    /// Live per-file/group bar fan-out bound; the actual in-flight-fetch cap is
    /// `gate`. Set from `--jobs` (min 1) so a bundle with many entries never
    /// instantiates more live progress bars than the run can actually service
    /// at once.
    jobs: usize,
    /// Global cap on concurrent blob fetches, one permit per group: a plain
    /// whole-file fetch, or a hint-carrying entry's complement-range fetch (plus
    /// its donor splice and any re-fetch), held as one logical fetch unit. A
    /// byte range a sibling entry already holds is spliced from disk instead of
    /// fetched, so it never takes a permit of its own.
    gate: tokio::sync::Semaphore,
    /// Per-provider cap on concurrent same-lane streams (`--max-lane-streams`,
    /// default 4). Acquired around every stream: one permit for a single-provider
    /// stream, a sorted permit set for a multi-source fan-out. Bounds only
    /// per-provider concurrency; `--jobs` still bounds cross-lane parallelism.
    lane_cap: LaneStreamCap,
    /// How many more rounds a retryably-failed entry gets after the first pass
    /// (`--entry-retries`). Each round re-probes and resumes the entry's
    /// `.partial`; `0` runs the single pass only.
    entry_retries: u32,
    /// The run's multi-bar progress renderer: one per-file bar per active pull
    /// above a bottom total bar (silent off a terminal or under `--json`). Set
    /// once the kept manifest is known — its entries decide the total-bar mode —
    /// so it starts [disabled](PullProgress::disabled) during the manifest fetch.
    progress: PullProgress,
}

impl<P: Provider + Clone> PullCtx<'_, P> {
    /// The shared gap-driven deps, assembled from `PullCtx`'s borrowed chain
    /// plumbing plus this run's per-fetch budgets — the same shape `decdn fetch`
    /// builds. `drive_fetch` bao-verifies every ingested byte and, on
    /// `finalize`, runs a whole-blob `valid_ranges` sweep. Shared by the
    /// single-source path and the multi-source pre-branch.
    fn drive_deps(&self, max_blob_bytes: u64) -> anyhow::Result<fetch::DriveFetchDeps<'_, P>> {
        Ok(fetch::DriveFetchDeps {
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
            deadlines: PullDeadlines::new(
                self.common.stall_timeout(),
                self.common.stall_timeout(),
                self.common.min_throughput_bps(),
            )?,
        })
    }

    /// The ADR 039 multi-source pre-branch for one entry (#1774). Returns
    /// `Ok(Some(()))` once the blob is fetched in parallel across the admitted
    /// set, `Ok(None)` when the engagement gate declines (the caller then runs
    /// the single-source failover loop), and `Err` when the fan-out engaged and
    /// failed.
    ///
    /// The bundle's `open_lock` is passed through so every lane's pool
    /// open-or-reuse still serializes against the other entries sharing the
    /// one on-chain pool. The pre-probe gate (kill switch, holder count,
    /// size-hint floor) runs before the fan-out: a fetch the gate declines
    /// never engages a lane.
    ///
    /// Every admitted provider's lane draws vouchers from the run's shared
    /// `LaneLedgers` (ADR 039): each `(pool_id, signer, provider)` lane has one
    /// monotonic issuer, so concurrent entries fanning out over the same
    /// provider issue vouchers off the same watermark instead of racing it.
    /// `--jobs 3` with overlapping provider sets runs those entries'
    /// transfers concurrently; only the per-lane voucher issuance
    /// serializes, not the transfer.
    ///
    /// `total` is the manifest's optional `size`, which the first open is cut
    /// from (#2063).
    async fn try_multi_source(
        &self,
        order: &fetch::ResolvedTargets,
        hash: [u8; 32],
        staging: &Path,
        total: Option<u64>,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<Option<()>> {
        // Admission is computed once and reused for the gate and the fan-out
        // itself — `try_multi_source_fetch` would otherwise recompute the
        // same `admit_sources` from `order.candidates`.
        let admitted = discovery::admit_sources(order.candidates.clone(), self.common.max_sources);
        if fetch::multi_source_gate_declines(
            self.common,
            &order.candidates,
            &admitted,
            order.size_hint,
        ) {
            return Ok(None);
        }
        // Hold one lane-stream permit per admitted provider across the whole
        // fan-out: every admitted lane opens a stream at once, so the cap must
        // admit the set together. Acquired in sorted `Address` order (deadlock
        // free) and only after the gate accepts, so a declined fetch takes none.
        let providers: Vec<Address> = admitted.iter().map(|c| c.eth_address).collect();
        let _lane_permits = self.lane_cap.permit_set(&providers).await?;
        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let deps = self.drive_deps(max_blob_bytes)?;
        // The entry's per-file bar callback (ADR 039 fan-out reports one monotonic
        // total the lanes fold their per-leg deltas into, so the bar never jumps
        // between lanes). The admitted set already computed for the gate is
        // moved into the fan-out so `admit_sources` runs only once.
        fetch::multi_source_download(
            &deps,
            self.common,
            self.grant.as_ref(),
            self.signer,
            self.voucher_dom,
            admitted,
            &order.coverage_by_node,
            self.relays,
            hash,
            staging,
            // The manifest's size, else the probed hint. Neither is signed: it
            // cuts the first open (#2063) and can decline fan-out early, and the
            // signed header decides the size.
            total.or(order.size_hint),
            progress,
            Some(&self.open_lock),
            Some(&self.ledgers),
        )
        .await
        .map(|opt| opt.map(|_bytes| ()))
    }

    /// Fetch one blob after explicit selection or discovery, streaming it into
    /// `staging` (#1497: the same [`fetch::drive_fetch`] gap-driven core `decdn
    /// fetch` uses, so bundle pull gets reactive top-up too), failing over across
    /// candidates on a retryable delivery failure (#1174, ADR 037 § Fallback).
    ///
    /// A pinned explicit node is its own only candidate — nothing to fail over
    /// to. Under discovery the entry is probed into the ordered failover list
    /// (proxy-warming non-holders first when they help, then holders nearest-RTT
    /// first) and walked in turn: a retryable failure advances to the next
    /// candidate, a terminal one stops, and the last error surfaces once the list
    /// is exhausted. Every attempt draws on the ONE shared pool and resumes the
    /// entry's `.partial` beside `staging`, so a fail-over re-pays nothing.
    ///
    /// `total` is the manifest's optional `size`, passed to the multi-source
    /// fan-out to cut its first open from (#2063).
    async fn fetch_to_staging(
        &self,
        hash: [u8; 32],
        staging: &Path,
        total: Option<u64>,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<()> {
        let _permit = self
            .gate
            .acquire()
            .await
            .map_err(|_| anyhow!("bundle pull concurrency gate closed"))?;
        if let Some(pinned) = self.explicit {
            // A `--node-id`-pinned target takes its direct address from `--addr`,
            // not the registry, so no on-chain dial hints apply.
            let _lane_permit = self.lane_cap.permit(pinned.1).await?;
            return self
                .fetch_to_staging_from(hash, pinned, &[], staging, progress)
                .await;
        }
        let candidates = self
            .candidates
            .as_deref()
            .ok_or_else(|| anyhow!("no discovery candidates available"))?;
        let order = fetch::probe_and_order(
            self.endpoint,
            candidates,
            self.relays.first(),
            hash,
            fetch::ProxyWarmingParams::from_args(self.common),
            self.slash_dom,
        )
        .await?;

        // Every bundle entry shares ONE `PaymentPool` deposit, but each lane's
        // voucher issuance draws on the run's shared `LaneLedgers` (ADR 039):
        // one monotonic issuer per `(pool_id, signer, provider)` lane keeps
        // concurrent `--jobs` entries on disjoint provider sets from jointly
        // over-issuing past the single deposit. Probes, transfers, and the
        // payment section below all run concurrently across entries; only the
        // per-lane voucher watermark is serialized, by the ledger itself.

        // Multi-source fan-out (ADR 039, #1774): the same pre-branch `decdn
        // fetch` runs. One entry engages N provider lanes at once, drawing
        // each lane's vouchers from the shared ledger, then fans out. The
        // engagement gate (kill switch off, too few admissible holders, below
        // the size floor) is decided inside `try_multi_source` and falls
        // through to the single-source failover loop below unchanged; a
        // retryable fan-out failure does the same, resuming the entry's
        // `.partial` so nothing paid for is re-bought.
        match self
            .try_multi_source(&order, hash, staging, total, progress)
            .await
        {
            Ok(Some(())) => return Ok(()),
            Ok(None) => {}
            Err(err)
                if retry_disposition(&err) == RetryDisposition::Terminal
                    || err.downcast_ref::<PoolExhausted>().is_some() =>
            {
                // On the delegated path reconnect a terminal exhaustion to
                // the owner remedy, same as the single-source path does.
                return Err(if self.grant.is_some() {
                    fetch::annotate_delegated_exhaustion(err)
                } else {
                    err
                });
            }
            Err(err) => {
                tracing::warn!(
                    "bundle pull: multi-source fetch of an entry failed ({err:#}); falling \
                     back to single-source failover over the same candidates"
                );
            }
        }

        walk_candidates(
            &order.candidates,
            &format!("entry {}", blake3::Hash::from_bytes(hash).to_hex()),
            |cand| cand.eth_address.to_string(),
            |cand| async move {
                let target = (cand.node_id, cand.eth_address);
                // Registry multiaddrs as direct-address hints for a relay-free
                // dial to a reachable node (ADR 001 § Node Discovery).
                let dial_addrs = cand.dial_addrs();
                // The failover walk streams from one provider per attempt, so it
                // holds just that provider's lane-stream permit for the attempt,
                // released before the next candidate.
                let _lane_permit = self.lane_cap.permit(cand.eth_address).await?;
                self.fetch_to_staging_from(hash, target, &dial_addrs, staging, progress)
                    .await
            },
        )
        .await
    }

    /// Build one entry's ready-to-drive [`PoolContext`] and dial target for
    /// `provider`, shared by the whole-blob ([`Self::fetch_to_staging_from`]) and
    /// ranged ([`Self::open_range_session`]) drive paths.
    ///
    /// Delegated (`--capability`): every entry adopts the named pool + owner
    /// capability (no on-chain open). Self-owned: open-or-reuse the caller's pool
    /// under the global open lock (every entry shares the one on-chain pool this
    /// bundle pulls from), then attach the ADR 005 client binding — without it the
    /// request carries no verified buyer identity, so the node's `pull_authorized`
    /// gate never fires a cache-miss origin pull and `--namespace` would be inert.
    /// The binding is signed outside `open_lock` (it touches no on-chain state).
    async fn build_pull_ctx(
        &self,
        (node_id, provider): FetchTarget,
        dial_addrs: &[std::net::SocketAddr],
    ) -> anyhow::Result<(PoolContext, EndpointAddr)> {
        let ctx = if let Some(grant) = &self.grant {
            fetch::build_delegated_pool_ctx(
                self.store,
                self.contract,
                self.signer,
                self.voucher_dom,
                provider,
                self.self_address,
                self.chain,
                self.endpoint,
                grant,
            )
            .await?
        } else {
            let ctx = {
                // The global pool lock: every entry — regardless of provider —
                // shares the one on-chain pool this bundle pulls from.
                let _open_guard = self.open_lock.lock().await;
                fetch::open_or_reuse_pool(
                    self.store,
                    self.contract,
                    self.rpc,
                    self.signer,
                    self.voucher_dom,
                    provider,
                    self.self_address,
                    self.chain.payment_pool,
                    self.chain.working_deposit,
                    self.chain.max_approve,
                    // `self.store` was opened from `self.chain.data_dir`, and
                    // the pull signs with `self.chain.keystore`.
                    ChainAdoption::for_buy(&self.chain.data_dir, &self.chain.keystore)?,
                )
                .await?
            };
            fetch::attach_client_binding(ctx, self.chain, self.endpoint, self.signer)?
        };

        let mut target = EndpointAddr::new(node_id);
        // `--addr` only applies to the explicit-node path (clap requires
        // `--node-id`); a discovered node is reached via its resolved address.
        if self.explicit.is_some()
            && let Some(addr) = self.common.addr
        {
            target = target.with_ip_addr(addr);
        }
        if let Some(url) = self.relays.first() {
            target = target.with_relay_url(url.clone());
        }
        // Registry-published direct addresses (empty on the pinned `--addr`
        // path): a reachable node connects without a relay (ADR 001 § Node
        // Discovery). Additive — a stale hint loses the path race, never fails it.
        for sock in dial_addrs {
            target = target.with_ip_addr(*sock);
        }
        Ok((ctx, target))
    }

    /// Fetch directly from `node_id`/`provider`, bypassing discovery, streaming
    /// into `staging`.
    async fn fetch_to_staging_from(
        &self,
        hash: [u8; 32],
        fetch_target: FetchTarget,
        dial_addrs: &[std::net::SocketAddr],
        staging: &Path,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<()> {
        let provider = fetch_target.1;
        let (ctx, target) = self.build_pull_ctx(fetch_target, dial_addrs).await?;

        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let deps = self.drive_deps(max_blob_bytes)?;

        // Immutable pool fact captured before `ctx` moves into `drive_fetch`.
        let pool_id = ctx.pool_id;

        // `drive_fetch` owns the `.partial` + `.obao4`/`.ranges` sidecars beside
        // `staging` and finalizes to the plain `staging` file this fetch's caller
        // (`materialize`/`fetch_to_memory`) reads. On error those sidecars are left
        // in place — the resume prefix a retried entry `open_or_create`s from. The
        // per-file `progress` callback advances this entry's bar (and folds its
        // bytes into the run's total bar); the bar's lifecycle is owned by the
        // caller (`fetch_group` / `pull_entry`), so the finish hook here is a
        // no-op — a mid-fetch fail-over must not clear the bar.
        let result = fetch::drive_fetch(
            &deps,
            ctx,
            target,
            provider,
            pool_id,
            hash,
            staging,
            progress,
            || {},
            Some(&self.ledgers),
        )
        .await;
        // On the delegated path a terminal owner-remedy reason (`SpendingCapExhausted`,
        // `CapabilityExpired`, `PoolExhausted`) reconnects to the owner-side remedy
        // (the delegate cannot self-resolve it), the same as `fetch`.
        match (result, self.grant.is_some()) {
            (Ok(_bytes), _) => Ok(()),
            (Err(err), true) => Err(fetch::annotate_delegated_exhaustion(err)),
            (Err(err), false) => Err(err),
        }
    }

    /// Open a range-drive session for `hash` against `node_id`/`provider`, for
    /// the entry whose ranged store is `entry_store` (#2119). The ranged twin of
    /// [`Self::fetch_to_staging_from`]: it builds the same context and target,
    /// then opens a [`fetch::RangeSession`] that every range drive of the entry
    /// reuses while this provider serves it. `first_ranges` are the ranges the
    /// session's first drive fills; the session opens that drive's first leg
    /// now, and opens nothing when they are empty.
    #[allow(clippy::too_many_arguments)]
    async fn open_range_session<'s>(
        &'s self,
        fetch_target: FetchTarget,
        dial_addrs: &[std::net::SocketAddr],
        hash: [u8; 32],
        staging: &Path,
        entry_store: &ClientRangedStore,
        first_ranges: &[(u64, u64)],
    ) -> anyhow::Result<fetch::RangeSession<'s, P>> {
        let provider = fetch_target.1;
        let (ctx, target) = self.build_pull_ctx(fetch_target, dial_addrs).await?;
        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let deps = self.drive_deps(max_blob_bytes)?;
        let pool_id = ctx.pool_id;
        fetch::RangeSession::open(
            &deps,
            ctx,
            target,
            provider,
            pool_id,
            hash,
            staging,
            entry_store,
            first_ranges,
            &self.ledgers,
        )
        .await
    }

    /// Resolve the provider order for one dedup entry's range drives ONCE — the
    /// pinned `--node-id`, or a single [`fetch::probe_and_order`] over the
    /// discovery candidates. [`Self::pull_entry`] resolves this before its first
    /// sub-drive and threads it into every one (the complement drive, a donor
    /// re-fetch, and any whole-blob re-drive), so a self-heal entry probes the
    /// candidate set once rather than up to three times. `total` is the
    /// manifest's size, which [`stripe_order`] checks each holder's coverage
    /// against.
    async fn resolve_range_targets(
        &self,
        hash: [u8; 32],
        total: u64,
    ) -> anyhow::Result<RangeTargets> {
        if let Some(pinned) = self.explicit {
            return Ok(RangeTargets::Pinned(pinned));
        }
        let candidates = self
            .candidates
            .as_deref()
            .ok_or_else(|| anyhow!("no discovery candidates available"))?;
        let resolved = fetch::probe_and_order(
            self.endpoint,
            candidates,
            self.relays.first(),
            hash,
            fetch::ProxyWarmingParams::from_args(self.common),
            self.slash_dom,
        )
        .await?;
        let (order, stripe) = stripe_order(
            resolved.candidates,
            &resolved.coverage_by_node,
            total,
            self.common.multi_source_enabled(),
            self.common.max_sources,
        );
        Ok(RangeTargets::Discovered { order, stripe })
    }

    /// Fetch one blob fully into memory — used only for the bundle manifest
    /// itself when named by `--hash` rather than read locally via `-i`.
    /// Manifests are small (unlike bundle entries, which stream straight to
    /// their destination and never buffer the whole blob), so streaming into a
    /// staging file under `out_root` and reading it back is cheap; it also
    /// means a manifest fetch gets the same reactive top-up as everything
    /// else. The staging file is removed once read back — a manifest fetch has
    /// nothing further to resume once its bytes are safely in memory.
    async fn fetch_to_memory(&self, hash: [u8; 32], out_root: &Path) -> anyhow::Result<Vec<u8>> {
        let staging = staging_path(out_root, hash)?;
        // The manifest blob fetch is silent (no bar): `progress` is disabled here
        // anyway, and the per-file bars belong to the entries, not the manifest.
        self.fetch_to_staging(hash, &staging, None, None).await?;
        let bytes =
            std::fs::read(&staging).with_context(|| format!("read {}", staging.display()))?;
        remove_staging_off_runtime(&staging).await;
        Ok(bytes)
    }

    /// Fetch every entry into `out_root`, one unit of work per *distinct* blob
    /// hash: entries sharing a hash (one file at two bundle paths) are fetched and
    /// reconstructed once, then materialized at each path (#1306) — never fetched,
    /// nor *paid for*, twice. Each unit of work runs via `buffer_unordered` (no
    /// `tokio::spawn`, by choice — nothing here is `!Send`), offered `--jobs` at
    /// a time so live per-file/group bars stay bounded; `PullCtx.gate` is the
    /// real cap on in-flight fetches, so parallelism comes from concurrent
    /// in-flight network I/O, while the shared `LaneLedgers` inside
    /// `fetch_to_staging_from` keep same-lane voucher issuance monotonic,
    /// scoped to each unique blob.
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
    ) -> (Vec<EntryOutcome>, Transfer) {
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
        // path is written rather than silently skipped.
        let disk = resolve_disk_state(entries, &saved, out_root, overwrite).await;
        for d in &disk.seed {
            index.seed_disk(d.hash, &d.source, d.offset, d.len);
        }
        // `saved` moves on to seed the incremental skip-cache flush: each
        // completed group's records fold onto it and rewrite the cache as the
        // pull progresses.
        let (outcomes, transfer) = self
            .pull_plain(
                &refs,
                out_root,
                overwrite,
                &index,
                &fetch_plan,
                &disk,
                saved,
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
        (outcomes, transfer)
    }

    /// Fetch every distinct blob once (grouped by hash) and materialize it at each
    /// destination path (#1306), routing each group through [`Self::pull_entry`] so a
    /// blob whose chunk hints overlap an already-materialized sibling pays only for
    /// the complement. Live per-group bars are bounded at `--jobs`; `PullCtx.gate`
    /// is the real cap on in-flight fetches.
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
    ) -> (Vec<EntryOutcome>, Transfer) {
        let groups_by_hash = order_groups_smallest_first(group_by_hash(entries));
        let group_count = groups_by_hash.len().max(1);
        // Completed groups' skip-cache records flow to the flush task over this
        // channel. Unbounded so a `send` from the fetch-driving side never blocks
        // (the flush write must never stall a fetch); the messages are one small
        // batch per completed group, so the channel stays shallow.
        let (flush_tx, flush_rx) = tokio::sync::mpsc::unbounded_channel::<FlushBatch>();

        // The fetch-driving side: run every group through `fetch_group` bounded by
        // `--jobs`, then re-run the groups that failed retryably for up to
        // `--entry-retries` more rounds (#2118). As each run completes, forward its
        // recordable files (built here while the group's entries are in scope) to
        // the flush task and keep its outcomes for the run summary; a retry
        // round's outcomes replace the round before.
        let drive = async {
            let mut groups: Vec<SettledGroup> = vec![SettledGroup::default(); groups_by_hash.len()];
            let run = |group: HashGroup<'e>| {
                // Cheap clone of the group's entry refs so `fetch_group` can consume
                // `group` while we still correlate outcomes to entries.
                let group_entries: Vec<&'e ManifestEntry> = group.entries.clone();
                // A group re-run in a retry round is not finished until it
                // finishes again, so a sibling deferring a chunk assigned to it
                // waits for it rather than paying for the chunk too. Cleared here,
                // as the round takes the group in order: an assigned fetcher is
                // smaller than its consumers, so it is taken first.
                if let Ok(whole) = fetch::parse_hash(group.hash) {
                    index.unmark_finished(whole);
                }
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
                    (run, updates)
                }
            };
            run_with_retries(
                groups_by_hash,
                // Live-bar fan-out bounded by --jobs; the global gate is the real
                // in-flight-fetch cap.
                self.jobs.min(group_count),
                self.entry_retries,
                run,
                |i, (run, updates)| settle_group_run(&mut groups, &flush_tx, i, run, updates),
            )
            .await;
            // Closing the channel tells the flush task to do its final write.
            drop(flush_tx);
            groups
        };

        // The flush task: fold each batch onto the prior skip-cache and rewrite it
        // atomically at the cadence, plus a final write when the channel closes.
        // Running here (joined, not awaited inside `drive`) keeps the blocking
        // write off the fetch-driving path — `join!` keeps polling `drive` while
        // this side awaits its write.
        let flush = flush_task(out_root, saved, flush_rx);

        let (groups, ()) = tokio::join!(drive, flush);
        // Byte tally is per-group (a blob pulled once, materialized to N paths),
        // so sum it before flattening away the group boundaries.
        let transfer = groups
            .iter()
            .map(|g| group_transfer(&g.outcomes, g.paid))
            .fold(Transfer::default(), Transfer::add);
        let outcomes = groups.into_iter().flat_map(|g| g.outcomes).collect();
        (outcomes, transfer)
    }

    /// Run [`Self::pull_entry_untimed`] and, when the entry lands, log one `-v`
    /// line with its size, the time the whole entry took, and its effective rate
    /// (#2120). Returns the content bytes the entry paid for (see
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
    ) -> anyhow::Result<u64> {
        let started = std::time::Instant::now();
        self.pull_entry_untimed(hash, hints, total, staging, index, fetch_plan, file)
            .await
            .inspect(|_| log_entry_done(hash, staging, started.elapsed()))
    }

    /// Reconstruct one entry's blob into `staging` (the finalized per-hash staging
    /// file [`Self::fetch_group`] then materializes to each destination), using chunk
    /// hints to dedup byte ranges against the run's [`ChunkIndex`] when they help.
    ///
    /// - No hints (or `total` unknown, or the blob is already finalized at
    ///   `staging`), or no chunk overlaps a materialized sibling → the plain
    ///   whole-file [`Self::fetch_to_staging`] path.
    /// - Otherwise the dedup path: pay through a [`CtxRangeDriver`] for only the
    ///   *complement* (the group-aligned bytes no donor covers) into `staging`'s
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
    /// can splice from this blob, and the content bytes the entry paid for are
    /// returned: the whole blob on the plain path, the blob less its spliced bytes
    /// on the dedup path, and 0 for an already-finalized staging blob. It is a
    /// content count — bao proof overhead is not in it, and a `.partial` prefix
    /// resumed from an earlier run counts again.
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
    ) -> anyhow::Result<u64> {
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
                return Ok(0);
            }
            remove_staging_off_runtime(staging).await;
        }

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
            // register its chunks so a *later* entry can dedup against it. The paid
            // count is the verified blob's length, not the manifest's optional
            // `size`: this path never checks `size`, so a wrong one still fetches.
            self.fetch_to_staging(hash, staging, total, progress)
                .await?;
            index.register(hints, staging);
            let paid = tokio::fs::metadata(staging).await.map_or(0, |m| m.len());
            return Ok(paid);
        };

        // Dedup path. Hold one fetch permit across the complement drive, the donor
        // splice, and any re-fetch — one logical fetch unit, exactly as
        // `fetch_to_staging` scopes its permit.
        let _permit = self
            .gate
            .acquire()
            .await
            .map_err(|_| anyhow!("bundle pull concurrency gate closed"))?;

        // Resolve the range-drive provider order ONCE for this entry and reuse it
        // across every sub-drive below (complement, donor re-fetch, whole-blob
        // re-drive) — the happy path (complement only) still probes exactly once.
        let targets = self.resolve_range_targets(hash, total).await?;
        // Hold one lane-stream permit per provider this entry may drive from across
        // every sub-drive (complement, donor re-fetch, whole-blob re-drive) and the
        // splice between them — one logical fetch unit — so the entry always keeps
        // a stream's place on its lane; each drive takes any extra permits it uses
        // only while it runs. Sorted `Address` order keeps it deadlock-free
        // against a fan-out entry's permit set.
        let _lane_permits = self.lane_cap.permit_set(&targets.providers()).await?;
        let driver = CtxRangeDriver::new(self, &targets, hash, staging, total, progress);

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
            hints,
            index,
            fetch_plan,
            file,
            &finish_progress,
        )
        .await?;
        self.dedup_stats
            .spliced_bytes
            .fetch_add(outcome.spliced_bytes, Ordering::Relaxed);
        self.dedup_stats
            .hints_ignored
            .fetch_add(outcome.hints_ignored, Ordering::Relaxed);
        Ok(total.saturating_sub(outcome.spliced_bytes))
    }

    /// Fetch the blob shared by one hash-group and write it under `out_root` at
    /// each entry's path. The blob is fetched — and paid for — **once**; the first
    /// writable destination receives the materialized bytes and every other is a
    /// hard link (or copy) of it (#1306). Returns one [`EntryOutcome`] per input
    /// entry, in order. Never panics or short-circuits — every failure becomes an
    /// [`EntryOutcome::Failed`], and the [`GroupRun`] says whether a later
    /// `--entry-retries` round may cure it.
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
        // hash fails every path in the group.
        let hash = match fetch::parse_hash(group.hash) {
            Ok(h) => h,
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

        // Staged once per group: `drive_fetch` finalizes to
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
            )
            .await;
        file_bar.finish();
        let paid = match fetched {
            Ok(paid) => paid,
            Err(e) => {
                // `pull_entry` (whole-file or dedup) leaves the `<hex>.partial` +
                // `.obao4`/`.ranges` sidecars in place on error — they are what the
                // next run resumes from rather than re-paying for bytes already landed
                // (same contract as `fetch`'s `<output>.partial` store). A retryable
                // failure goes into the next `--entry-retries` round, which resumes
                // from them.
                return GroupRun::fetch_failed(slots, &e);
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

        GroupRun::landed(outcomes, paid)
    }
}

/// Log a finished entry's size, time and effective rate at `-v` (#2120), so a
/// run's slow entries can be told from its fast ones. The time covers the whole
/// entry: probing, every drive, any splice and the whole-file check.
fn log_entry_done(hash: [u8; 32], staging: &Path, elapsed: std::time::Duration) {
    let bytes = std::fs::metadata(staging).map_or(0, |m| m.len());
    let secs = elapsed.as_secs_f64();
    let rate = if secs > 0.0 {
        fetch::fmt_rate(fetch::bytes_as_f64(bytes) / secs)
    } else {
        fetch::fmt_rate(0.0)
    };
    tracing::info!(
        "bundle pull: {}: {} in {:.1}s ({rate})",
        blake3::Hash::from_bytes(hash).to_hex(),
        indicatif::HumanBytes(bytes),
        secs,
    );
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
}

/// The production [`RangeDriver`]: the live paid range-drive path over a
/// pre-resolved provider order (so repeated sub-drives of one entry share one
/// probe round).
///
/// Every drive of the entry — the pay-now complement, a donor re-fetch, each
/// deferred fallback, the self-heal re-drive — reuses the entry's open
/// [`fetch::RangeSession`]s while their providers serve: one warm connection
/// per provider for the entry, not one per drive or per range (#2119). The
/// drives stripe their ranges across the entry's admitted full holders, one
/// session each (#2123); with one holder, or multi-source off, one session
/// serves them. A lane that faults retires, and the entry does not go back to
/// a discovered provider that failed it (a pinned `--node-id` is its only
/// candidate); a retry round (`--entry-retries`) starts a new driver over a
/// fresh probe. Each lane fills up to `--max-lane-streams` gaps at once, using
/// the permits for its provider that are free when the drive starts beyond
/// the one the entry already holds. It returns those extra permits when the
/// drive ends: the entry keeps waiting on siblings between drives, and a
/// sibling it waits on may need them.
struct CtxRangeDriver<'a, P: Provider + Clone> {
    hash: [u8; 32],
    staging: &'a Path,
    walk: SessionWalk<LiveSessions<'a, P>>,
}

impl<'a, P: Provider + Clone> CtxRangeDriver<'a, P> {
    /// A driver for one entry, with no session open yet.
    fn new(
        ctx: &'a PullCtx<'a, P>,
        targets: &'a RangeTargets,
        hash: [u8; 32],
        staging: &'a Path,
        total: u64,
        progress: Option<&'a ProgressCallback>,
    ) -> Self {
        Self {
            hash,
            staging,
            walk: SessionWalk::new(LiveSessions {
                ctx,
                pinned: matches!(targets, RangeTargets::Pinned(_)),
                stripe: targets.stripe(),
                targets: targets.targets(),
                hash,
                staging,
                total,
                progress,
                store: std::sync::OnceLock::new(),
                topups_used: std::sync::atomic::AtomicU32::new(0),
            }),
        }
    }
}

impl<P: Provider + Clone> RangeDriver for CtxRangeDriver<'_, P> {
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
        Box::pin(self.walk.drive(ranges))
    }
}

/// How a [`RangeSessions::drive`] over the open sessions ended.
struct LanesDriven {
    /// Per open session, in the order given: whether its lane faulted.
    faulted: Vec<bool>,
    /// The drive's result.
    result: anyhow::Result<()>,
}

/// The candidates a [`SessionWalk`] drives an entry's ranges from.
trait RangeSessions {
    /// One candidate's open session.
    type Session;

    /// How many candidates there are, in failover order.
    fn candidates(&self) -> usize;

    /// How many of the first candidates a drive stripes across at once. The
    /// rest are failover reserve, opened one at a time once no striped
    /// session is left. `1` is plain single-source failover.
    fn stripe(&self) -> usize;

    /// Whether the one candidate was pinned (`--node-id`) rather than probed.
    fn pinned(&self) -> bool;

    /// Candidate `index`'s name, for the failover warning.
    fn label(&self, index: usize) -> String;

    /// The entry's name, for the failover warning.
    fn entry(&self) -> String;

    /// Open a session against candidate `index`, whose first drive fills
    /// `first_ranges`. Empty `first_ranges` open no first leg.
    async fn open(
        &self,
        index: usize,
        first_ranges: &[(u64, u64)],
    ) -> anyhow::Result<Self::Session>;

    /// Drive `ranges` through every open session at once. Each is
    /// `(candidate index, session, takes the drive's first gap)`.
    async fn drive(
        &self,
        open: &[(usize, &Self::Session, bool)],
        ranges: &[(u64, u64)],
    ) -> LanesDriven;
}

/// An entry's walk over its range-drive candidates (#2119, #2123): one session
/// per provider, reused by every drive of the entry while that provider
/// serves.
///
/// A drive runs across every open session. It opens the stripe
/// ([`RangeSessions::stripe`]) up front and, once no striped session is left,
/// one reserve candidate at a time. A session whose lane faults is dropped,
/// and a failure no single lane owns (the drive-level floor) drops every open
/// session. Whatever the drive left missing, it drives again on what is still
/// open or next in line, so one call returns only when the ranges are filled,
/// a terminal fault stops them, or no candidate is left. The entry does not go
/// back to a discovered provider that failed it. A pinned candidate is its own
/// only one, so a call reopens it after a failure dropped its session. A
/// terminal fault or a local disk fault ends the walk at once: no provider can
/// fix either. The open sessions and the next candidate sit under one lock.
///
/// Only the last session a pass opens opens the drive's first leg, and that
/// session takes the first gap: a pull primed early would go stale while the
/// other sessions open.
struct SessionWalk<S: RangeSessions> {
    sessions: S,
    state: tokio::sync::Mutex<WalkState<S::Session>>,
}

/// Record `err` as the walk's last error, unless the error it holds already
/// rules out a retry round and `err` does not ([`entry_retryable`]): a later
/// transient open failure must not turn a size mismatch or a pool exhaustion
/// into a round that fails the same way.
fn keep_walk_error(last: &mut Option<anyhow::Error>, err: anyhow::Error) {
    let keep_old = last
        .as_ref()
        .is_some_and(|old| !entry_retryable(old) && entry_retryable(&err));
    if keep_old {
        tracing::debug!("bundle pull: a later candidate also failed: {err:#}");
    } else {
        *last = Some(err);
    }
}

/// A [`SessionWalk`]'s open sessions and where its walk resumes.
struct WalkState<T> {
    /// The open sessions and their candidate indices.
    open: Vec<(usize, T)>,
    /// The first candidate not yet opened: every earlier one is open or
    /// failed.
    next: usize,
    /// The open session whose source holds the next drive's first leg.
    primed: Option<usize>,
}

impl<S: RangeSessions> SessionWalk<S> {
    fn new(sessions: S) -> Self {
        Self {
            sessions,
            state: tokio::sync::Mutex::new(WalkState {
                open: Vec::new(),
                next: 0,
                primed: None,
            }),
        }
    }

    /// Drive `ranges` from the open sessions, failing over down the rest.
    async fn drive(&self, ranges: &[(u64, u64)]) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let n = self.sessions.candidates();
        let pinned = self.sessions.pinned();
        if pinned && state.open.is_empty() {
            state.next = 0;
        }
        let entry = self.sessions.entry();
        let failed_before = state.next.saturating_sub(state.open.len());
        let what = if failed_before == 0 {
            format!("the ranges of entry {entry}")
        } else {
            format!(
                "the ranges of entry {entry} ({failed_before} earlier candidate(s) already \
                 failed it)"
            )
        };
        let mut last_err: Option<anyhow::Error> = None;
        loop {
            self.fill(&mut state, ranges, &what, &mut last_err).await?;
            if state.open.is_empty() {
                return Err(match last_err {
                    Some(err) if pinned => err,
                    Some(err) => {
                        tracing::warn!(
                            "bundle pull: no provider could deliver {what} ({err:#}); every one \
                             of {n} candidate(s) has now failed"
                        );
                        err.context(format!(
                            "all {n} candidate provider(s) failed to deliver {what}"
                        ))
                    }
                    None => anyhow!("no candidate node could deliver {what}"),
                });
            }
            let driven = {
                let primed = state.primed.take();
                let lanes: Vec<(usize, &S::Session, bool)> = state
                    .open
                    .iter()
                    .map(|(index, session)| (*index, session, primed == Some(*index)))
                    .collect();
                self.sessions.drive(&lanes, ranges).await
            };
            let failed: Vec<String> = state
                .open
                .iter()
                .zip(&driven.faulted)
                .filter(|&(_, faulted)| *faulted)
                .map(|((index, _), _)| self.sessions.label(*index))
                .collect();
            // A session with no report is treated as faulted: it is never
            // reused on a guess.
            let mut faulted = driven.faulted.iter();
            state
                .open
                .retain(|_| !faulted.next().copied().unwrap_or(true));
            let err = match driven.result {
                Ok(()) => return Ok(()),
                Err(err) => err,
            };
            // No provider can fix a terminal fault or a local disk fault, so
            // neither fails over.
            if retry_disposition(&err) == RetryDisposition::Terminal || is_local_disk_fault(&err) {
                return Err(err);
            }
            let failed = if failed.is_empty() {
                // No lane owns the failure: every open session failed it.
                let every = state
                    .open
                    .iter()
                    .map(|(index, _)| self.sessions.label(*index))
                    .collect();
                state.open.clear();
                every
            } else {
                failed
            };
            if state.next < n || !state.open.is_empty() {
                tracing::warn!(
                    "bundle pull: provider(s) {} could not deliver {what} ({err:#}); failing \
                     over to the rest of {n} candidate(s)",
                    failed.join(", "),
                );
            }
            keep_walk_error(&mut last_err, err);
        }
    }

    /// Open the sessions the next drive needs: every stripe member not yet
    /// tried, and, when none is open, the next reserve candidate. A candidate
    /// whose session will not open is skipped for good.
    async fn fill(
        &self,
        state: &mut WalkState<S::Session>,
        ranges: &[(u64, u64)],
        what: &str,
        last_err: &mut Option<anyhow::Error>,
    ) -> anyhow::Result<()> {
        let n = self.sessions.candidates();
        let stripe = self.sessions.stripe().max(1);
        loop {
            let end = if state.next < stripe {
                stripe.min(n)
            } else if state.open.is_empty() {
                state.next.saturating_add(1).min(n)
            } else {
                state.next
            };
            if end <= state.next {
                return Ok(());
            }
            while state.next < end {
                let index = state.next;
                state.next = index.saturating_add(1);
                let primes = state.primed.is_none() && state.next == end;
                let first: &[(u64, u64)] = if primes { ranges } else { &[] };
                match self.sessions.open(index, first).await {
                    Ok(session) => {
                        if primes {
                            state.primed = Some(index);
                        }
                        state.open.push((index, session));
                    }
                    Err(err) => {
                        if retry_disposition(&err) == RetryDisposition::Terminal {
                            return Err(err);
                        }
                        if state.next < n || !state.open.is_empty() {
                            tracing::warn!(
                                "bundle pull: provider {} could not deliver {what} ({err:#}); \
                                 failing over to the rest of {n} candidate(s)",
                                self.sessions.label(index),
                            );
                        }
                        keep_walk_error(last_err, err);
                    }
                }
            }
        }
    }
}

/// The live [`RangeSessions`]: paid [`fetch::RangeSession`]s against the
/// entry's pinned or probed candidates, all filling the entry's one ranged
/// store.
struct LiveSessions<'a, P: Provider + Clone> {
    ctx: &'a PullCtx<'a, P>,
    pinned: bool,
    stripe: usize,
    targets: Vec<(FetchTarget, Vec<std::net::SocketAddr>)>,
    hash: [u8; 32],
    staging: &'a Path,
    total: u64,
    progress: Option<&'a ProgressCallback>,
    /// The entry's ranged store beside `staging`, opened at the first session
    /// open. Every lane of every drive writes into this one store, so one
    /// in-memory present set and one writer own the `.ranges` record.
    store: std::sync::OnceLock<ClientRangedStore>,
    /// The entry's reactive top-ups, one budget across every lane.
    topups_used: std::sync::atomic::AtomicU32,
}

impl<P: Provider + Clone> LiveSessions<'_, P> {
    /// Candidate `index`'s dial target and hints.
    fn target(&self, index: usize) -> anyhow::Result<&(FetchTarget, Vec<std::net::SocketAddr>)> {
        self.targets
            .get(index)
            .ok_or_else(|| anyhow!("no range-drive candidate {index}"))
    }

    /// The entry's ranged store, opened on first use.
    fn entry_store(&self) -> anyhow::Result<&ClientRangedStore> {
        if let Some(store) = self.store.get() {
            return Ok(store);
        }
        let opened = fetch::open_entry_store(self.staging, self.hash, self.total)?;
        Ok(self.store.get_or_init(|| opened))
    }

    /// On the delegated path a terminal owner-remedy reason reconnects to the
    /// owner-side remedy (the delegate cannot self-resolve it), the same as
    /// `fetch`.
    fn annotate(&self, err: anyhow::Error) -> anyhow::Error {
        if self.ctx.grant.is_some() {
            fetch::annotate_delegated_exhaustion(err)
        } else {
            err
        }
    }
}

impl<'a, P: Provider + Clone> RangeSessions for LiveSessions<'a, P> {
    type Session = fetch::RangeSession<'a, P>;

    fn candidates(&self) -> usize {
        self.targets.len()
    }

    fn stripe(&self) -> usize {
        self.stripe
    }

    fn pinned(&self) -> bool {
        self.pinned
    }

    fn label(&self, index: usize) -> String {
        self.targets.get(index).map_or_else(
            || format!("#{index}"),
            |((_, provider), _)| provider.to_string(),
        )
    }

    fn entry(&self) -> String {
        blake3::Hash::from_bytes(self.hash).to_hex().to_string()
    }

    async fn open(
        &self,
        index: usize,
        first_ranges: &[(u64, u64)],
    ) -> anyhow::Result<Self::Session> {
        let (target, dial_addrs) = self.target(index)?;
        self.ctx
            .open_range_session(
                *target,
                dial_addrs,
                self.hash,
                self.staging,
                self.entry_store()?,
                first_ranges,
            )
            .await
            .map_err(|err| self.annotate(err))
    }

    async fn drive(
        &self,
        open: &[(usize, &Self::Session, bool)],
        ranges: &[(u64, u64)],
    ) -> LanesDriven {
        let unfaulted = |result: anyhow::Result<()>| LanesDriven {
            faulted: vec![false; open.len()],
            result,
        };
        let store = match self.entry_store() {
            Ok(store) => store,
            Err(err) => return unfaulted(Err(err)),
        };
        // Held for this drive only, never across the entry's waits on a sibling
        // (`reconcile_deferred`): a donor this entry waits on may be blocked on
        // the same providers' permits.
        let mut extras = Vec::with_capacity(open.len());
        let mut lanes = Vec::with_capacity(open.len());
        for &(index, session, takes_first) in open {
            let provider = match self.target(index) {
                Ok(((_, provider), _)) => *provider,
                Err(err) => return unfaulted(Err(err)),
            };
            let extra = self
                .ctx
                .lane_cap
                .try_extra(provider, self.ctx.lane_cap.n.saturating_sub(1))
                .await;
            lanes.push(fetch::StripeLane {
                session,
                width: std::num::NonZeroUsize::MIN.saturating_add(extra.len()),
                takes_first,
            });
            extras.push(extra);
        }
        let driven = Box::pin(fetch::drive_stripe(
            store,
            &lanes,
            ranges,
            self.progress,
            &self.topups_used,
        ))
        .await;
        drop(extras);
        LanesDriven {
            faulted: driven.faulted,
            result: driven.result.map_err(|err| self.annotate(err)),
        }
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
/// `.ranges` record and truncates a `.partial` that lacks one — wiping every
/// byte already spliced into it. A store that already has its record (a drive
/// made it, or a resumed run) is reopened as is. The data file is then extended
/// to `total`, sparsely, because the whole-file hash reads its full length.
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
/// Returns the entry's [`DedupOutcome`] — bytes actually spliced from disk and hints
/// dropped by a fault — for the run-level report. A resume that finalized on the
/// first pay-now drive spliced nothing this run; a self-heal whole-blob re-drive
/// discards every splice, so it reports zero spliced bytes and counts all its donors
/// as ignored.
#[allow(clippy::too_many_arguments)]
async fn reassemble_dedup(
    driver: &dyn RangeDriver,
    plan: &ReassemblePlan,
    total: u64,
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
        // drive can COMPLETE the store — `drive` then ran its whole-blob bao sweep
        // against `hash` and renamed `<hex>.partial` -> `<hex>`. The blob is
        // finalized and verified; splicing would open a `.partial` that no longer
        // exists. Register the donor chunks and return. No splice ran this run.
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome::default());
        }
    }

    let partial = ensure_partial(staging, hash, total)?;

    // Verify + splice each initially-available donor range off the executor. A donor
    // whose chunk no longer hashes to its hint (a lying donor hint, or a short read)
    // is re-fetched normally rather than trusted.
    // Each re-fetched donor is a dropped hint.
    let (mut spliced_bytes, refetch) = splice_off_runtime(&partial, plan.donor.clone()).await?;
    let mut hints_ignored = u64::try_from(refetch.len()).unwrap_or(u64::MAX);
    if !refetch.is_empty() {
        driver.drive(&refetch).await?;
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome {
                spliced_bytes,
                hints_ignored,
            });
        }
    }

    // Tail reconcile: splice each deferred (sibling-assigned) range as its fetcher
    // registers it, waiting on the index's progress signal. A deferred chunk whose
    // assigned fetcher finishes WITHOUT producing it (a failed fetcher) is driven
    // and paid here instead, so the run never hangs.
    let (tail_spliced, tail_ignored) =
        reconcile_deferred(driver, &partial, &plan.deferred, index, fetch_plan, file).await?;
    spliced_bytes = spliced_bytes.saturating_add(tail_spliced);
    hints_ignored = hints_ignored.saturating_add(tail_ignored);
    if staging.try_exists()? {
        finish_progress();
        index.register(hints, staging);
        return Ok(DedupOutcome {
            spliced_bytes,
            hints_ignored,
        });
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
        // count as ignored.
        spliced_bytes = 0;
        hints_ignored =
            u64::try_from(plan.donor.len().saturating_add(plan.deferred.len())).unwrap_or(u64::MAX);
        tracing::warn!(
            "bundle pull: entry {} failed its whole-file hash after range-dedup; \
             re-fetching the whole blob",
            blake3::Hash::from_bytes(hash).to_hex()
        );
        driver.drive(&[(0, total)]).await?;
        // The whole-blob re-drive covers `[0, total)`, so `drive` finalized it: its
        // bao sweep verified the bytes against `hash` and renamed `.partial` ->
        // `staging`. The blob is verified — do not re-hash a `.partial` that no
        // longer exists; register the donor chunks and return.
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome {
                spliced_bytes,
                hints_ignored,
            });
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
    Ok(DedupOutcome {
        spliced_bytes,
        hints_ignored,
    })
}

/// Tail reconcile for one entry's deferred (sibling-assigned) chunks: splice each
/// as its assigned fetcher registers it in `index`, waiting on the index progress
/// signal between scans rather than polling on a clock. A deferred chunk whose
/// assigned fetcher FINISHES without registering it (a failed fetcher, or one with
/// no recorded assignee) — or one registered under a length that disagrees with the
/// hint — is driven and paid here instead, over the whole groups its span touches,
/// so the entry always completes and the run never hangs. Returns `(spliced_bytes,
/// hints_ignored)`: bytes served from a splice, and deferred chunks that fell back to
/// a paid drive.
async fn reconcile_deferred(
    driver: &dyn RangeDriver,
    partial: &Path,
    deferred: &[DeferredChunk],
    index: &ChunkIndex,
    fetch_plan: &FetchPlan,
    file: Option<&pull_progress::FileBar>,
) -> anyhow::Result<(u64, u64)> {
    let mut waiting: Vec<DeferredChunk> = deferred.to_vec();
    let mut spliced_bytes = 0u64;
    let mut ignored = 0u64;
    if waiting.is_empty() {
        return Ok((0, 0));
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
            let (spliced, refetch) = splice_off_runtime(partial, ready).await?;
            spliced_bytes = spliced_bytes.saturating_add(spliced);
            ignored = ignored.saturating_add(u64::try_from(refetch.len()).unwrap_or(u64::MAX));
            fallback.extend(refetch);
        }

        if !fallback.is_empty() {
            driver.drive(&fallback).await?;
            // A fallback drive may complete the store (`drive` renames
            // `.partial` -> `<hex>`); no more splicing is then possible or needed.
            if driver.staging().try_exists()? {
                return Ok((spliced_bytes, ignored));
            }
        }

        waiting = still;
        if waiting.is_empty() {
            return Ok((spliced_bytes, ignored));
        }

        // Nothing to do this pass but wait on a sibling — surface it on the file row
        // (its download is done; the total bar shows the run is still moving).
        if let Some(f) = file {
            f.set_pending();
        }
        // Block until the next registration or group-finish, then rescan.
        notified.as_mut().await;
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

    /// Take back [`Self::mark_finished`] for a group that runs again in a retry
    /// round.
    fn unmark_finished(&self, whole: [u8; 32]) {
        self.finished
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&whole);
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

/// [`splice_donors`] on the blocking pool. Returns the bytes spliced — the
/// donors' `dst` spans less those of the donors that could not be trusted — and
/// the [`DonorRange::refetch`] span of each untrusted donor, which the caller
/// drives and pays for.
async fn splice_off_runtime(
    partial: &Path,
    donors: Vec<DonorRange>,
) -> anyhow::Result<(u64, Vec<(u64, u64)>)> {
    let span_total = |ds: &[DonorRange]| ds.iter().map(|d| d.dst.1).fold(0u64, u64::saturating_add);
    let planned = span_total(&donors);
    let partial = partial.to_path_buf();
    let failed = off_runtime("donor splice", move || splice_donors(&partial, &donors)).await?;
    let spliced = planned.saturating_sub(span_total(&failed));
    Ok((spliced, failed.iter().map(|d| d.refetch).collect()))
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
/// materializes at each destination — the same shape `drive_fetch`'s own finalize
/// leaves for the whole-file path.
fn promote_partial(partial: &Path, staging: &Path) -> anyhow::Result<()> {
    std::fs::rename(partial, staging)
        .with_context(|| format!("promote {} -> {}", partial.display(), staging.display()))?;
    for sidecar in [
        staging.with_extension("partial.obao4"),
        staging.with_extension("partial.ranges"),
    ] {
        if let Err(e) = std::fs::remove_file(&sidecar)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                "failed to remove staging sidecar {}: {e}",
                sidecar.display()
            );
        }
    }
    Ok(())
}

/// Per-hash staging file [`fetch::drive_fetch`] finalizes to before `fetch_group`
/// materializes it at the manifest's destination path(s) —
/// `<out_root>/.decdn-partial/<hex>` (#1497). This is the *plain* final path:
/// `drive_fetch` owns the `.partial` suffix and the `.obao4`/`.ranges` sidecars
/// itself, streaming into `<hex>.partial` beside this and renaming to `<hex>` on
/// `finalize`. Those sidecars are what survives an interrupted pull — a rerun
/// resumes from them rather than re-paying for bytes already landed. Creates the
/// staging directory (idempotent, and only ever called once a group has real
/// work to do — an all-skipped group never creates one) but not the file itself;
/// `drive_fetch` does that.
/// Reserved subdirectory of `out_root` holding per-hash staging files and their
/// sidecars. Manifest entry paths that resolve inside it are rejected in
/// [`plan_slots`], so a manifest can never collide with a staging file — nor
/// trick `remove_staging` into deleting a materialized output.
const STAGING_DIR: &str = ".decdn-partial";

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
/// any leftover `<hex>.partial{,.obao4,.ranges}` sidecars: `drive_fetch`'s
/// `finalize` normally clears those on success, but this is the belt to that
/// brace and never runs on an errored entry (whose sidecars are the resume
/// prefix a retry needs). A leftover here is harmless clutter, not a
/// correctness issue, so a removal failure is reported and swallowed rather
/// than propagated.
fn remove_staging(staging: &Path) {
    let paths_to_remove = [
        staging.to_path_buf(),
        staging.with_extension("partial"),
        staging.with_extension("partial.obao4"),
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

/// Print the would-fetch plan and exit (no network/chain/keystore activity).
/// With `--hash` the entries can't be enumerated offline, so only the intent is
/// reported. The `--include`/`--exclude` `filter` is applied to a local
/// manifest's entries so the plan reflects exactly what a real run would fetch.
fn dry_run(args: &BundlePullArgs, filter: &EntryFilter, filters_given: bool) -> anyhow::Result<()> {
    let out = args.output.display();
    match (&args.input, &args.hash) {
        (Some(path), _) => {
            let mut manifest = read_local_manifest(path)?;
            let raw_empty = manifest.entries.is_empty();
            manifest.entries = filter.apply(manifest.entries);
            if args.json {
                // The `--json` plan stays machine-readable — an empty set is
                // `count: 0` with an empty `entries` array, no prose line.
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
        }
        (None, Some(h)) => {
            let filter_note = if filters_given {
                " (the include/exclude filters then apply)"
            } else {
                ""
            };
            println!(
                "--dry-run with --hash: would fetch bundle {h} then its entries into {out} \
                 (entries are not enumerable without fetching the manifest){filter_note}"
            );
        }
        (None, None) => bail!("no bundle source (expected -i or --hash)"),
    }
    Ok(())
}

/// Summarize outcomes; return an error if any entry failed (after reporting all).
fn report(
    outcomes: &[EntryOutcome],
    transfer: Transfer,
    dedup: DedupSummary,
    output: &Path,
    json: bool,
) -> anyhow::Result<()> {
    let mut fetched = 0u64;
    let mut linked = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;
    let mut deduped = 0u64;
    let mut reused_bytes = 0u64;
    for o in outcomes {
        match o {
            EntryOutcome::Fetched(_) => fetched += 1,
            EntryOutcome::Linked => linked += 1,
            EntryOutcome::Skipped => skipped += 1,
            EntryOutcome::Deduped(n) => {
                deduped += 1;
                reused_bytes = reused_bytes.saturating_add(*n);
            }
            EntryOutcome::Failed { path, err } => {
                failed += 1;
                // A per-entry failure is a command result the user needs, not
                // routing narration: keep it on stderr (unconditional, and clear
                // of the `--json` report on stdout) rather than behind logging.
                eprintln!("failed: {path}: {err}");
            }
        }
    }

    let rep = PullReport {
        output: output.display().to_string(),
        fetched,
        linked,
        skipped,
        failed,
        downloaded: transfer.downloaded,
        reconstructed: transfer.reconstructed,
        spliced_bytes: dedup.spliced_bytes,
        hints_ignored: dedup.hints_ignored,
        deduped,
        reused_bytes,
    };
    if json {
        let line = serde_json::to_string(&rep).map_err(|e| anyhow!("serialize report: {e}"))?;
        println!("{line}");
    } else {
        println!(
            "pulled into {} ({fetched} fetched, {linked} linked, {skipped} skipped, \
             {deduped} deduped, {failed} failed)",
            output.display()
        );
        // `downloaded X → reconstructed Y` only when dedup made them differ;
        // otherwise a single `downloaded X`.
        println!("{}", transfer_line(transfer));
        // The whole-file dedup outcome, shown only when it mattered: a run that
        // materialized any destination from an on-disk donor instead of fetching.
        if deduped > 0 {
            println!(
                "whole-file dedup: reused {} from disk ({deduped} file(s))",
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
/// once (`downloaded` = `paid`, the content bytes its fetch paid for, else the
/// size of the single `Fetched`) or reused from an on-disk donor with no download
/// (`Deduped`, contributing 0 to `downloaded`) — a group never mixes the two,
/// since [`PullCtx::fetch_group`] takes one path or the other. Every materialized
/// copy — the canonical (`Fetched` or `Deduped`) plus each `Linked` duplicate
/// path — is a full file on disk (`reconstructed` = size × copies). A group with
/// nothing written (all skipped or failed) contributes nothing.
fn group_transfer(outcomes: &[EntryOutcome], paid: Option<u64>) -> Transfer {
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
                downloaded: paid_size.map_or(0, |size| paid.unwrap_or(size)),
                reconstructed: n.saturating_mul(copies),
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
mod tests {
    use super::*;

    fn exhausted() -> anyhow::Error {
        anyhow::Error::new(PoolExhausted {
            gap_start: 0,
            gap_len: 1,
        })
    }

    fn terminal() -> anyhow::Error {
        anyhow::Error::new(decdn_client::BlobTooLarge {
            received: 1 << 40,
            ceiling: 1 << 20,
        })
    }

    /// The walk stops at the first success and never tries the rest.
    #[tokio::test]
    async fn walk_candidates_stops_at_the_first_success() {
        let tried = std::sync::Mutex::new(Vec::new());
        let got = walk_candidates(&[1, 2, 3], "x", u8::to_string, |c: &u8| {
            tried.lock().expect("lock").push(*c);
            let c = *c;
            async move {
                if c == 2 {
                    Ok(c)
                } else {
                    Err(anyhow!("fault {c}"))
                }
            }
        })
        .await
        .expect("the second candidate delivers");
        assert_eq!(got, 2);
        assert_eq!(*tried.lock().expect("lock"), vec![1, 2]);
    }

    /// A terminal error stops the walk and surfaces unchanged: another provider
    /// would fail the same way.
    #[tokio::test]
    async fn walk_candidates_stops_on_a_terminal_error() {
        let tried = std::sync::Mutex::new(Vec::new());
        let err = walk_candidates(&[1u8, 2], "x", u8::to_string, |c: &u8| {
            tried.lock().expect("lock").push(*c);
            async { Err::<(), _>(terminal()) }
        })
        .await
        .expect_err("terminal");
        assert_eq!(*tried.lock().expect("lock"), vec![1]);
        assert!(err.downcast_ref::<decdn_client::BlobTooLarge>().is_some());
        assert!(
            !format!("{err:#}").contains("candidate provider"),
            "{err:#}"
        );
    }

    /// A pool exhaustion fails over within the walk: a cheaper provider may fit
    /// what the deposit holds (`retry_disposition`, #1174). Both the whole-file
    /// and the range walks follow this one rule (#2118).
    #[tokio::test]
    async fn walk_candidates_fails_over_on_pool_exhaustion() {
        let tried = std::sync::Mutex::new(Vec::new());
        walk_candidates(&[1u8, 2], "x", u8::to_string, |c: &u8| {
            tried.lock().expect("lock").push(*c);
            let c = *c;
            async move { if c == 1 { Err(exhausted()) } else { Ok(()) } }
        })
        .await
        .expect("the second candidate delivers");
        assert_eq!(*tried.lock().expect("lock"), vec![1, 2]);
    }

    /// Running out of candidates names that fact in the error the run summary
    /// prints, and the original error still classifies through the context.
    #[tokio::test]
    async fn walk_candidates_says_when_every_candidate_failed() {
        let err = walk_candidates(&[1u8, 2, 3], "the entry", u8::to_string, |_: &u8| async {
            Err::<(), _>(exhausted())
        })
        .await
        .expect_err("every candidate fails");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("all 3 candidate provider(s) failed to deliver the entry"),
            "{msg}"
        );
        assert!(err.downcast_ref::<PoolExhausted>().is_some());
        assert_eq!(retry_disposition(&err), RetryDisposition::RetryElsewhere);
    }

    #[tokio::test]
    async fn walk_candidates_with_no_candidates_errors() {
        let none: [u8; 0] = [];
        let err = walk_candidates(&none, "the entry", u8::to_string, |_: &u8| async { Ok(()) })
            .await
            .expect_err("nothing to try");
        assert!(format!("{err}").contains("no candidate node"), "{err}");
    }

    /// Only a failure a later round can cure goes into it. A spent pool is
    /// excluded: every provider already refused the deposit.
    #[test]
    fn entry_retryable_excludes_terminal_and_exhausted_failures() {
        assert!(entry_retryable(&anyhow!("early eof")));
        assert!(!entry_retryable(&terminal()));
        assert!(!entry_retryable(&exhausted()));
        assert!(!entry_retryable(
            &exhausted().context("all 2 candidate provider(s) failed")
        ));
        assert!(
            !entry_retryable(&anyhow::Error::new(decdn_client::SignedSizeMismatch {
                signed: 2,
                expected: 1,
            })),
            "every provider signs the same size, so a round would repeat it"
        );
    }

    #[test]
    fn entry_retry_backoff_doubles_to_a_cap() {
        let secs: Vec<u64> = (1..=6).map(|r| entry_retry_backoff(r).as_secs()).collect();
        assert_eq!(secs, vec![2, 4, 8, 16, 30, 30]);
        assert_eq!(entry_retry_backoff(u32::MAX).as_secs(), 30);
    }

    /// Items that ask for a retry run again, up to the round limit, after the
    /// backoff; a later result replaces the earlier one; and an item that
    /// succeeds drops out of the next round.
    #[tokio::test(start_paused = true)]
    async fn run_with_retries_reruns_only_the_items_that_ask() {
        // Item `i` fails its first `i` runs.
        let runs = std::sync::Mutex::new(vec![0u32; 4]);
        let mut last = vec![None; 4];
        let started = tokio::time::Instant::now();
        run_with_retries(
            vec![0usize, 1, 2, 3],
            2,
            2,
            |i| {
                let n = {
                    let mut runs = runs.lock().expect("lock");
                    let slot = runs.get_mut(i).expect("item");
                    *slot += 1;
                    *slot
                };
                async move { n > u32::try_from(i).expect("small") }
            },
            |i, ok: bool| {
                *last.get_mut(i).expect("item") = Some(ok);
                !ok
            },
        )
        .await;
        // Item 3 needs a fourth run, but two retry rounds allow three.
        assert_eq!(*runs.lock().expect("lock"), vec![1, 2, 3, 3]);
        assert_eq!(last, vec![Some(true), Some(true), Some(true), Some(false)]);
        // Two rounds waited 2 s then 4 s.
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(6));
    }

    /// `--entry-retries 0` runs the single pass and never waits.
    #[tokio::test(start_paused = true)]
    async fn run_with_retries_zero_runs_once() {
        let runs = std::sync::atomic::AtomicU32::new(0);
        let started = tokio::time::Instant::now();
        run_with_retries(
            vec![()],
            1,
            0,
            |()| {
                runs.fetch_add(1, Ordering::Relaxed);
                async { false }
            },
            |_, ok: bool| !ok,
        )
        .await;
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
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

    fn one_update(
        path: &str,
        hash: &str,
        size: u64,
    ) -> BTreeMap<String, bundle_manifest::SavedFile> {
        let mut m = BTreeMap::new();
        m.insert(path.to_string(), saved_file(hash, size));
        m
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
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + 'a>>
            {
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

        let res = reassemble_dedup(
            &driver,
            &plan,
            2 * GROUP,
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

    /// A donor that failed the first pass and runs again in an `--entry-retries`
    /// round splices a chunk it was assigned but a recipient has since paid for
    /// and registered, instead of paying for it a second time: a registered
    /// chunk is a donor whoever the fetch plan assigned it to.
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
        assert_eq!(outcome.spliced_bytes, cut);
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
        assert_eq!(outcome.spliced_bytes, cut);
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
            Transfer::default(),
            DedupSummary::default(),
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
                Transfer::default(),
                DedupSummary::default(),
                Path::new("/out"),
                false
            )
            .is_ok()
        );
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
            }),
            "downloaded 27.6 GB"
        );
        // Differ (dedup saved bytes) → both figures with the arrow.
        assert_eq!(
            transfer_line(Transfer {
                downloaded: 13_800_000_000,
                reconstructed: 27_600_000_000,
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
        let t = group_transfer(&outcomes, Some(100));
        assert_eq!(t.downloaded, 100);
        assert_eq!(t.reconstructed, 300);
    }

    /// A range-dedup fetch pays only for the bytes it did not splice from disk,
    /// so `downloaded` is its paid tally, not the blob size — while every copy on
    /// disk is still a whole file.
    #[test]
    fn group_transfer_downloads_only_the_paid_bytes_of_a_spliced_blob() {
        let outcomes = vec![EntryOutcome::Fetched(100), EntryOutcome::Linked];
        let t = group_transfer(&outcomes, Some(30));
        assert_eq!(t.downloaded, 30);
        assert_eq!(t.reconstructed, 200);

        // A blob already finalized in staging by an earlier run pays nothing.
        let t = group_transfer(&[EntryOutcome::Fetched(100)], Some(0));
        assert_eq!(t.downloaded, 0);
        assert_eq!(t.reconstructed, 100);
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

    fn filtered_paths(
        include: &[&str],
        exclude: &[&str],
        entries: Vec<ManifestEntry>,
    ) -> Vec<String> {
        let filter = EntryFilter::compile(&strs(include), &strs(exclude)).unwrap();
        filter.apply(entries).into_iter().map(|e| e.path).collect()
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
        let claimed =
            "b3:0000000000000000000000000000000000000000000000000000000000000000".to_string();
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
                Transfer::default(),
                DedupSummary::default(),
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

        let none = T::try_parse_from(["t", "-o", "out", "--hash", "b3:aa"])
            .expect("namespace is optional");
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

    /// A scripted [`RangeSessions`]: counts opens and drives per candidate and
    /// fails the lanes listed in `fail` as `(candidate, nth lane-drive)`.
    struct FakeSessions {
        candidates: usize,
        stripe: usize,
        pinned: bool,
        /// `(candidate, nth lane-drive)` pairs whose lane faults.
        fail: Vec<(usize, usize)>,
        /// Drive calls (1-based) that fail with no lane at fault: a floor trip.
        stall: Vec<usize>,
        /// Drive calls (1-based) whose first lane faults with this error.
        lane_error: Vec<(usize, fn() -> anyhow::Error)>,
        fail_open: Vec<usize>,
        opens: std::sync::Mutex<Vec<usize>>,
        /// Candidates that opened with a first leg.
        primed_opens: std::sync::Mutex<Vec<usize>>,
        /// One entry per lane per drive, in drive order.
        drives: std::sync::Mutex<Vec<usize>>,
        /// Per drive call: the candidates it ran on, and which took the first
        /// gap.
        calls: std::sync::Mutex<Vec<(Vec<usize>, Option<usize>)>>,
    }

    impl FakeSessions {
        fn new(candidates: usize) -> Self {
            Self {
                candidates,
                stripe: 1,
                pinned: false,
                fail: Vec::new(),
                stall: Vec::new(),
                lane_error: Vec::new(),
                fail_open: Vec::new(),
                opens: std::sync::Mutex::new(Vec::new()),
                primed_opens: std::sync::Mutex::new(Vec::new()),
                drives: std::sync::Mutex::new(Vec::new()),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl RangeSessions for FakeSessions {
        type Session = usize;

        fn candidates(&self) -> usize {
            self.candidates
        }

        fn stripe(&self) -> usize {
            self.stripe
        }

        fn pinned(&self) -> bool {
            self.pinned
        }

        fn label(&self, index: usize) -> String {
            format!("p{index}")
        }

        fn entry(&self) -> String {
            "e".into()
        }

        async fn open(&self, index: usize, first: &[(u64, u64)]) -> anyhow::Result<usize> {
            self.opens.lock().unwrap().push(index);
            if self.fail_open.contains(&index) {
                bail!("open {index} refused");
            }
            if !first.is_empty() {
                self.primed_opens.lock().unwrap().push(index);
            }
            Ok(index)
        }

        async fn drive(&self, open: &[(usize, &usize, bool)], _: &[(u64, u64)]) -> LanesDriven {
            let call = {
                let mut calls = self.calls.lock().unwrap();
                calls.push((
                    open.iter().map(|&(i, _, _)| i).collect(),
                    open.iter()
                        .find(|&&(_, _, first)| first)
                        .map(|&(i, _, _)| i),
                ));
                calls.len()
            };
            if let Some((_, make)) = self.lane_error.iter().find(|(c, _)| *c == call) {
                let mut faulted = vec![false; open.len()];
                if let Some(first) = faulted.first_mut() {
                    *first = true;
                }
                return LanesDriven {
                    faulted,
                    result: Err(make()),
                };
            }
            let faulted: Vec<bool> = open
                .iter()
                .map(|&(index, session, _)| {
                    assert_eq!(*session, index, "a drive uses its candidate's session");
                    let nth = {
                        let mut drives = self.drives.lock().unwrap();
                        drives.push(index);
                        drives.len()
                    };
                    self.fail.contains(&(index, nth))
                })
                .collect();
            let result = if self.stall.contains(&call) {
                Err(anyhow!("drive {call} stalled"))
            } else if faulted.iter().all(|&f| f) {
                Err(anyhow!("drive {call} failed on every lane"))
            } else {
                Ok(())
            };
            LanesDriven { faulted, result }
        }
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

    /// The stripe is the full holders, one per operator and at most
    /// `max_sources`, first in the order; every other candidate follows as
    /// reserve in its own order.
    #[test]
    fn stripe_order_puts_admitted_full_holders_first() {
        let total = 4 * decdn_protocol::DISCOVERY_BLOCK_BYTES;
        let blocks = decdn_protocol::num_blocks(total);
        // 1 is a proxy (no coverage), 2 and 3 full holders of one operator, 4
        // a partial holder, 5 and 6 full holders of their own operators.
        let order: Vec<NodeCandidate> = [(1, 1), (2, 2), (3, 2), (4, 4), (5, 5), (6, 6)]
            .into_iter()
            .map(|(n, o)| stripe_candidate(n, o))
            .collect();
        let mut coverage = HashMap::new();
        for n in [2, 3, 5, 6] {
            coverage.insert(
                stripe_candidate(n, 0).node_id,
                decdn_protocol::Coverage::full(blocks),
            );
        }
        coverage.insert(
            stripe_candidate(4, 0).node_id,
            decdn_protocol::Coverage::from_block_indices(blocks, 0..1),
        );
        let ids = |o: &[NodeCandidate]| -> Vec<PublicKey> { o.iter().map(|c| c.node_id).collect() };
        let expect = |nodes: &[u8]| -> Vec<PublicKey> {
            nodes
                .iter()
                .map(|&n| stripe_candidate(n, 0).node_id)
                .collect()
        };

        let (striped, stripe) = stripe_order(order.clone(), &coverage, total, true, 2);
        assert_eq!(stripe, 2);
        assert_eq!(ids(&striped), expect(&[2, 5, 1, 3, 4, 6]));

        let (all, stripe) = stripe_order(order.clone(), &coverage, total, true, 8);
        assert_eq!(stripe, 3, "one per operator: 2 and 3 share one");
        assert_eq!(ids(&all), expect(&[2, 5, 6, 1, 3, 4]));
    }

    /// With multi-source off, or fewer than two admitted full holders, the
    /// order is unchanged and there is no stripe.
    #[test]
    fn stripe_order_falls_back_to_plain_failover() {
        let total = 4 * decdn_protocol::DISCOVERY_BLOCK_BYTES;
        let blocks = decdn_protocol::num_blocks(total);
        let order: Vec<NodeCandidate> = (1..=3).map(|n| stripe_candidate(n, n)).collect();
        let mut coverage = HashMap::new();
        for n in 1..=3 {
            coverage.insert(
                stripe_candidate(n, 0).node_id,
                decdn_protocol::Coverage::full(blocks),
            );
        }
        let ids: Vec<PublicKey> = order.iter().map(|c| c.node_id).collect();
        let (same, stripe) = stripe_order(order.clone(), &coverage, total, false, 4);
        assert_eq!(
            (same.iter().map(|c| c.node_id).collect::<Vec<_>>(), stripe),
            (ids.clone(), 1)
        );

        let one_holder: HashMap<_, _> = coverage.into_iter().take(1).collect();
        let (same, stripe) = stripe_order(order, &one_holder, total, true, 4);
        assert_eq!(
            (same.iter().map(|c| c.node_id).collect::<Vec<_>>(), stripe),
            (ids, 1)
        );
    }

    /// Every drive of an entry reuses the one session while its provider serves.
    #[tokio::test]
    async fn session_walk_reuses_one_session_across_drives() {
        let walk = SessionWalk::new(FakeSessions::new(3));
        for _ in 0..3 {
            walk.drive(&[(0, 1)]).await.unwrap();
        }
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0]);
        assert_eq!(*walk.sessions.drives.lock().unwrap(), vec![0, 0, 0]);
    }

    /// A failed drive fails over, and later drives start at the provider that
    /// took over: the entry never goes back to the one that failed it.
    #[tokio::test]
    async fn session_walk_does_not_go_back_to_a_failed_provider() {
        let mut fake = FakeSessions::new(3);
        fake.fail = vec![(0, 2)];
        let walk = SessionWalk::new(fake);
        walk.drive(&[(0, 1)]).await.unwrap();
        walk.drive(&[(0, 1)]).await.unwrap();
        walk.drive(&[(0, 1)]).await.unwrap();
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1]);
        assert_eq!(*walk.sessions.drives.lock().unwrap(), vec![0, 0, 1, 1]);
    }

    /// A candidate whose session will not open is skipped for good too, and a
    /// walk with no candidate left errors without opening anything.
    #[tokio::test]
    async fn session_walk_runs_out_and_says_so() {
        let mut fake = FakeSessions::new(2);
        fake.fail_open = vec![0, 1];
        let walk = SessionWalk::new(fake);
        let err = walk.drive(&[(0, 1)]).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("all 2 candidate provider(s)"),
            "{err:#}"
        );
        let err = walk.drive(&[(0, 1)]).await.unwrap_err();
        assert!(format!("{err}").contains("no candidate node"), "{err}");
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1]);
    }

    /// A pinned candidate is the only one: a failed drive reopens it next time.
    #[tokio::test]
    async fn session_walk_keeps_driving_a_pinned_candidate() {
        let mut fake = FakeSessions::new(1);
        fake.pinned = true;
        fake.fail = vec![(0, 1)];
        let walk = SessionWalk::new(fake);
        let err = walk.drive(&[(0, 1)]).await.unwrap_err();
        assert!(
            !format!("{err:#}").contains("candidate provider(s)"),
            "a pinned failure is not a walk that ran out: {err:#}"
        );
        walk.drive(&[(0, 1)]).await.unwrap();
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 0]);
    }

    /// A striped walk opens the whole stripe up front and drives every
    /// session at once. Only the last session it opens opens the first leg,
    /// and that session takes the first gap.
    #[tokio::test]
    async fn session_walk_opens_the_stripe_and_drives_it_at_once() {
        let mut fake = FakeSessions::new(4);
        fake.stripe = 3;
        let walk = SessionWalk::new(fake);
        walk.drive(&[(0, 1)]).await.unwrap();
        walk.drive(&[(0, 1)]).await.unwrap();
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(*walk.sessions.primed_opens.lock().unwrap(), vec![2]);
        assert_eq!(
            *walk.sessions.calls.lock().unwrap(),
            vec![(vec![0, 1, 2], Some(2)), (vec![0, 1, 2], None)],
            "the primed session takes only the first drive's first gap"
        );
    }

    /// A faulted lane leaves the stripe for good while the others keep
    /// serving, and the reserve opens only once no striped session is left.
    #[tokio::test]
    async fn session_walk_drops_a_faulted_lane_and_falls_back_to_the_reserve() {
        let mut fake = FakeSessions::new(3);
        fake.stripe = 2;
        // Drive 1: lanes 0 and 1 (entries 1, 2); lane 0 faults. Drive 2: lane
        // 1 alone (entry 3) faults, so the reserve opens (entry 4).
        fake.fail = vec![(0, 1), (1, 3)];
        let walk = SessionWalk::new(fake);
        walk.drive(&[(0, 1)]).await.unwrap();
        walk.drive(&[(0, 1)]).await.unwrap();
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1, 2]);
        let calls: Vec<Vec<usize>> = walk
            .sessions
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(lanes, _)| lanes.clone())
            .collect();
        assert_eq!(calls, vec![vec![0, 1], vec![1], vec![2]]);
    }

    /// A failure no lane owns, such as the drive-level floor, fails over every
    /// open session.
    #[tokio::test]
    async fn session_walk_fails_every_lane_over_on_a_drive_level_failure() {
        let mut fake = FakeSessions::new(3);
        fake.stripe = 2;
        fake.stall = vec![1];
        let walk = SessionWalk::new(fake);
        walk.drive(&[(0, 1)]).await.unwrap();
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1, 2]);
        let calls: Vec<Vec<usize>> = walk
            .sessions
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(lanes, _)| lanes.clone())
            .collect();
        assert_eq!(calls, vec![vec![0, 1], vec![2]]);
    }

    /// A terminal fault ends the walk at once, unwrapped: no reserve candidate
    /// opens, because no provider can fix it.
    #[tokio::test]
    async fn session_walk_stops_on_a_terminal_fault() {
        let mut fake = FakeSessions::new(4);
        fake.stripe = 2;
        fake.lane_error = vec![(1, || {
            anyhow::Error::new(decdn_client::BlobTooLarge {
                received: 2,
                ceiling: 1,
            })
        })];
        let walk = SessionWalk::new(fake);
        let err = walk.drive(&[(0, 1)]).await.unwrap_err();
        assert!(err.downcast_ref::<decdn_client::BlobTooLarge>().is_some());
        assert!(
            !format!("{err:#}").contains("candidate provider(s)"),
            "{err:#}"
        );
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1]);
    }

    /// A local disk fault ends the walk too: every provider would meet it.
    #[tokio::test]
    async fn session_walk_stops_on_a_local_disk_fault() {
        let mut fake = FakeSessions::new(3);
        fake.lane_error = vec![(1, || {
            anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::StorageFull))
        })];
        let walk = SessionWalk::new(fake);
        let err = walk.drive(&[(0, 1)]).await.unwrap_err();
        assert!(is_local_disk_fault(&err), "{err:#}");
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0]);
    }

    /// The walk keeps an error that rules out a retry round over a later one
    /// that would allow it, and otherwise keeps the latest.
    #[test]
    fn keep_walk_error_keeps_the_error_that_rules_out_a_round() {
        let mismatch = || {
            anyhow::Error::new(decdn_client::SignedSizeMismatch {
                signed: 2,
                expected: 1,
            })
        };
        let mut last = None;
        keep_walk_error(&mut last, anyhow!("connect refused"));
        keep_walk_error(&mut last, mismatch());
        keep_walk_error(&mut last, anyhow!("connect refused"));
        assert!(
            last.as_ref().is_some_and(|e| e
                .downcast_ref::<decdn_client::SignedSizeMismatch>()
                .is_some()),
            "a transient error does not replace the size mismatch"
        );
        let mut last = Some(anyhow!("first"));
        keep_walk_error(&mut last, anyhow!("second"));
        assert_eq!(format!("{}", last.unwrap()), "second");
    }

    /// A stripe member that will not open is skipped, and the stripe runs on
    /// the rest. When the last planned open fails, no session opens the first
    /// leg for that drive.
    #[tokio::test]
    async fn session_walk_stripes_across_the_members_that_open() {
        let mut fake = FakeSessions::new(3);
        fake.stripe = 2;
        fake.fail_open = vec![1];
        let walk = SessionWalk::new(fake);
        walk.drive(&[(0, 1)]).await.unwrap();
        assert_eq!(*walk.sessions.opens.lock().unwrap(), vec![0, 1]);
        assert!(
            walk.sessions.primed_opens.lock().unwrap().is_empty(),
            "the last planned open failed, so no first leg was opened"
        );
        assert_eq!(*walk.sessions.calls.lock().unwrap(), vec![(vec![0], None)]);
    }

    /// A group that runs again in a retry round is no longer finished until it
    /// finishes again, so a sibling deferring a chunk assigned to it waits for
    /// it rather than paying for the chunk too.
    #[test]
    fn a_retried_group_is_not_finished_until_it_finishes_again() {
        let index = ChunkIndex::default();
        index.mark_finished([0xaa; 32]);
        assert!(index.is_finished([0xaa; 32]));
        index.unmark_finished([0xaa; 32]);
        assert!(!index.is_finished([0xaa; 32]));
        index.mark_finished([0xaa; 32]);
        assert!(index.is_finished([0xaa; 32]));
    }

    /// A retry round's outcome replaces the round before; recordable files
    /// flush only when there are any; the retry flag passes through.
    #[test]
    fn settle_group_run_replaces_the_earlier_round() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut groups = vec![SettledGroup::default(), SettledGroup::default()];
        let failed = GroupRun::fetch_failed(
            vec![Slot::Write {
                label: "a.bin",
                dest: PathBuf::from("a.bin"),
            }],
            &anyhow!("stream reset"),
        );
        assert!(failed.retry, "a peer fault goes into the next round");
        assert!(settle_group_run(
            &mut groups,
            &tx,
            1,
            failed,
            BTreeMap::new()
        ));
        assert!(matches!(
            groups[1].outcomes.as_slice(),
            [EntryOutcome::Failed { .. }]
        ));
        assert_eq!(groups[1].paid, None, "a failed round paid for nothing");
        assert!(rx.try_recv().is_err(), "nothing recorded, nothing flushed");

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
        let landed = GroupRun::landed(vec![EntryOutcome::Fetched(3)], 2);
        assert!(!settle_group_run(&mut groups, &tx, 1, landed, updates));
        assert!(matches!(
            groups[1].outcomes.as_slice(),
            [EntryOutcome::Fetched(3)]
        ));
        assert_eq!(
            groups[1].paid,
            Some(2),
            "the landed round's paid tally is kept"
        );
        assert_eq!(rx.try_recv().expect("a flush batch").fetched_bytes, 3);
    }

    /// A materialize-stage failure keeps its paid blob for the next run and is
    /// never retried in this one; nor is a local disk fault or a manifest size
    /// no provider signs.
    #[test]
    fn only_a_fixable_fetch_failure_is_retried() {
        let slot = || {
            vec![Slot::Write {
                label: "a.bin",
                dest: PathBuf::from("a.bin"),
            }]
        };
        assert!(!GroupRun::done(fail_all(slot(), &anyhow!("materialize"))).retry);
        let disk = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::StorageFull))
            .context("write staging");
        assert!(!GroupRun::fetch_failed(slot(), &disk).retry);
        assert!(!entry_retryable(&anyhow::Error::new(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        ))));
        assert!(entry_retryable(&anyhow::Error::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        ))));
    }

    /// `--entry-retries` defaults to 2 and accepts `0`.
    #[test]
    fn entry_retries_flag_defaults_to_two() {
        use clap::Parser as _;
        #[derive(clap::Parser)]
        struct T {
            #[command(flatten)]
            args: BundlePullArgs,
        }
        let base = ["t", "-o", "out", "--hash", "b3:aa"];
        assert_eq!(T::try_parse_from(base).unwrap().args.entry_retries, 2);
        let off = T::try_parse_from(base.iter().copied().chain(["--entry-retries", "0"])).unwrap();
        assert_eq!(off.args.entry_retries, 0);
    }

    /// `try_extra` takes only the permits free right now, never more than asked
    /// and never past the cap, and they return to the pool when dropped.
    #[tokio::test]
    async fn lane_stream_cap_try_extra_takes_only_free_permits() {
        let p1 = Address::repeat_byte(1);
        let cap = LaneStreamCap::new(4);
        let held = cap.permit(p1).await.unwrap();
        let extra = cap.try_extra(p1, 3).await;
        assert_eq!(extra.len(), 3, "the three free permits");
        assert!(cap.try_extra(p1, 3).await.is_empty(), "the cap is reached");
        drop(extra);
        assert_eq!(cap.try_extra(p1, 2).await.len(), 2, "no more than asked");
        drop(held);
        assert_eq!(cap.try_extra(p1, 10).await.len(), 4, "never past the cap");
    }

    #[tokio::test]
    async fn lane_stream_cap_serializes_one_provider_and_frees_the_rest() {
        use std::time::Duration;

        let p1 = Address::repeat_byte(1);
        let p2 = Address::repeat_byte(2);

        // n == 1: the first permit for P1 is held; a second acquire for the same
        // provider must not resolve until the first drops.
        let cap = LaneStreamCap::new(1);
        let held = cap.permit(p1).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(100), cap.permit(p1)).await;
        assert!(
            second.is_err(),
            "a second same-provider permit must block while the first is held"
        );
        // A different provider never contends.
        let other = tokio::time::timeout(Duration::from_millis(100), cap.permit(p2)).await;
        assert!(
            other.is_ok(),
            "a distinct provider must not wait on P1's permit"
        );
        // Dropping the first lets the waiter through.
        drop(held);
        let reacquired = tokio::time::timeout(Duration::from_millis(100), cap.permit(p1))
            .await
            .expect("the second permit must resolve once the first is dropped")
            .unwrap();
        drop(reacquired);

        // n == 2: two permits for the same provider coexist.
        let cap2 = LaneStreamCap::new(2);
        let a = cap2.permit(p1).await.unwrap();
        let b = tokio::time::timeout(Duration::from_millis(100), cap2.permit(p1))
            .await
            .expect("two same-provider permits must coexist at n == 2")
            .unwrap();
        drop((a, b));

        // permit_set over an out-of-order, duplicated set acquires every distinct
        // provider (in sorted order internally) and returns one permit each.
        let cap3 = LaneStreamCap::new(1);
        let permits = cap3.permit_set(&[p2, p1, p2]).await.unwrap();
        assert_eq!(permits.len(), 2, "duplicates collapse to one permit each");
        // With both lanes held, a fresh single acquire for either must block.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), cap3.permit(p1))
                .await
                .is_err(),
            "P1 is held by the set"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), cap3.permit(p2))
                .await
                .is_err(),
            "P2 is held by the set"
        );
        drop(permits);
    }
}
