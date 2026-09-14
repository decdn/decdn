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
//! or a hint-carrying entry's complement-range fetch — fans out concurrently,
//! bounded by one global `--jobs` cap (`PullCtx.gate`). A manifest `chunks`
//! entry is never fetched or stored as its own blob: its hints only let a
//! byte range shared with another entry be recognized and spliced from disk
//! instead of paid for again.
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

use std::collections::{HashMap, HashSet};
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

use super::chain_ctx;
use super::fetch;
use super::manifest::build_glob_set;
use super::pull_progress::{self, PullProgress};
use decdn_bao_range::CHUNK_GROUP_BYTES;
use decdn_client_pull::discovery::{self, NodeCandidate};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::provider;
use decdn_client_pull::{
    ClientRangedStore, LaneLedgers, PoolContext, PoolExhausted, ProgressCallback, PullDeadlines,
    RetryDisposition, retry_disposition,
};

type FetchTarget = (PublicKey, Address);

/// A dedup entry's resolved range-drive provider order, computed once per entry
/// and reused across its complement drive, donor re-fetch, and any whole-blob
/// re-drive (so a self-heal entry probes once, not per sub-drive). `Pinned` is
/// the `--node-id` target — its own only candidate; `Discovered` is the probed
/// discovery order walked with single-source failover.
enum RangeTargets {
    Pinned(FetchTarget),
    Discovered(Vec<NodeCandidate>),
}

impl RangeTargets {
    /// The distinct provider addresses this entry's range drives may stream from —
    /// the pinned target's one provider, or every discovered candidate's. Used to
    /// acquire the entry's lane-stream permit set before its drives.
    fn providers(&self) -> Vec<Address> {
        match self {
            RangeTargets::Pinned((_, provider)) => vec![*provider],
            RangeTargets::Discovered(order) => order.iter().map(|c| c.eth_address).collect(),
        }
    }
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

/// Materialize `src`'s content at `dest` without re-reading it over the network:
/// a hard link where the filesystem allows it, else a full copy (cross-device
/// `EXDEV`, or a filesystem that can't link). Staged in `dest`'s parent and
/// renamed into place so `dest` is only ever absent or complete — the same
/// atomic-replace invariant [`materialize`] upholds, which bundle pull's
/// skip-existing relies on ("a present final file is verified-good").
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

/// Resolve each entry's on-disk destination and classify it — a resolve failure,
/// an already-present file to skip, or a path to write — before any fetch.
/// Skip-existing (default): a present final file is verified-good (renamed into
/// place only after a BLAKE3 check), so re-runs resume. Evaluated **per
/// destination**, so one path of a duplicated blob can be skipped while another
/// is written.
fn plan_slots<'a>(
    entries: &[&'a ManifestEntry],
    out_root: &Path,
    overwrite: bool,
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
            Ok(dest) if !overwrite && dest.try_exists().unwrap_or(false) => Slot::Skip,
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
/// — the core of #1306 — is unit-testable without a live endpoint or pool.
async fn materialize_group<MF, MFut, LF>(
    slots: Vec<Slot<'_>>,
    mut materialize: MF,
    link: LF,
) -> Vec<EntryOutcome>
where
    MF: FnMut(PathBuf) -> MFut,
    MFut: std::future::Future<Output = anyhow::Result<u64>>,
    LF: Fn(&Path, &Path) -> anyhow::Result<()>,
{
    let mut canonical: Option<PathBuf> = None;
    let mut outcomes = Vec::with_capacity(slots.len());
    for slot in slots {
        let outcome = match slot {
            Slot::Failed(o) => o,
            Slot::Skip => EntryOutcome::Skipped,
            Slot::Write { label, dest } => match &canonical {
                Some(src) => match link(src, &dest) {
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

/// A group of manifest entries that all name the same blob `hash` — one file
/// published at two (or more) bundle paths. Non-empty by construction; the
/// shared `hash` is carried explicitly so consumers never re-derive it from an
/// arbitrary member.
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
    Failed { path: String, err: String },
}

/// One-line `--json` summary. `fetched`/`linked`/`skipped`/`failed` are entry
/// counts; `downloaded` is the whole-file content bytes of each distinct blob
/// fetched (a blob shared across several paths counts once, #1306) and
/// `reconstructed` is the total bytes written to disk this run — they diverge
/// when one blob is materialized to several paths. `downloaded` is a content-size
/// tally, not an exact on-wire measurement: it excludes bao proof overhead, counts
/// a chunk served from an existing staging file on a resumed run, and counts a
/// blob's whole size even when range-dedup paid for only its complement — so it
/// equals `reconstructed` per single-path blob and does not report chunk savings.
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

/// A pull's byte accounting: `downloaded` is the whole-file content bytes of each
/// distinct blob fetched (a blob materialized to several paths counts once, #1306);
/// `reconstructed` is the total bytes written to disk (every materialized copy).
/// The two are equal unless one blob serves several paths. `downloaded` sums whole
/// content lengths, not exact on-wire bytes: it omits bao proof overhead, still
/// counts a chunk resumed from staging, and counts a blob's whole size even when
/// range-dedup paid for only its complement — it tracks distinct blobs fetched, not
/// the bytes range-dedup saved, so it does not fall below `reconstructed` on a
/// single-path blob whose chunks were spliced from a sibling.
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
                report_nothing_to_fetch(filters_given && !raw_empty);
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

    let store = RedbBuyerPoolStore::open(&chain.data_dir)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;

    // Selection + the buyer signer, resolved per path (see `resolve_selection`).
    let Selection {
        explicit,
        candidates,
        signer,
        self_address,
    } = resolve_selection(common, &chain).await?;

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
    let namespace_id = args
        .namespace
        .map_or(decdn_protocol::client::NO_NAMESPACE, |n| {
            alloy::primitives::U256::from(n).to_be_bytes()
        });

    let mut ctx = PullCtx {
        endpoint: &endpoint,
        store: &store,
        contract: &contract,
        rpc: &rpc,
        signer: &signer,
        self_address,
        token,
        voucher_dom: &voucher_dom,
        slash_dom: &slash_dom,
        chain: &chain,
        relays: &relays,
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
        // Silent during the manifest fetch below (a single blob); replaced once
        // the kept entries are known and their sizes decide the total-bar mode.
        progress: PullProgress::disabled(),
    };

    // Obtain the manifest: the pre-read local one, or the `--hash` bundle blob
    // fetched and filtered here. `None` means it filtered down to empty (already
    // reported), so the run is done.
    let Some(manifest) =
        obtain_manifest(&ctx, args, &filter, filters_given, local_manifest).await?
    else {
        return Ok(());
    };

    // The kept manifest is known: enable the multi-bar renderer (silent off a
    // terminal or under `--json`). Its total bar's denominator is the run's whole
    // content size, fixed now from the manifest's declared sizes.
    ctx.progress = PullProgress::new(args.json, total_content_bytes(&manifest.entries));

    let (outcomes, transfer) = ctx
        .pull_all(&manifest.entries, &args.output, args.overwrite)
        .await;
    ctx.progress.finish();

    // Every entry has joined, so the shared dedup counters are now stable.
    let dedup = DedupSummary {
        spliced_bytes: ctx.dedup_stats.spliced_bytes.load(Ordering::Relaxed),
        hints_ignored: ctx.dedup_stats.hints_ignored.load(Ordering::Relaxed),
    };
    report(&outcomes, transfer, dedup, &args.output, args.json)
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
    let bytes = ctx
        .fetch_to_memory(hash, &args.output)
        .await
        .context("fetch bundle manifest blob")?;
    let mut m = parse_manifest(&bytes)?;
    let raw_empty = m.entries.is_empty();
    m.entries = filter.apply(m.entries);
    if m.entries.is_empty() {
        report_nothing_to_fetch(filters_given && !raw_empty);
        return Ok(None);
    }
    Ok(Some(m))
}

/// Per-provider cap on concurrent streams to one `(pool, signer, provider)`
/// lane. A lane has one shared [`LaneLedgers`] voucher watermark; two concurrent
/// streams on the same lane race that watermark — a fast stream advances it and
/// a slow co-stream's vouchers fall behind — so this bounds how many streams
/// touch a given provider at once. `--max-lane-streams` sets the cap (default 1):
/// at 1 a `Semaphore(1)` runs a single ordered voucher sequence per lane, and a
/// higher value admits that many concurrent same-lane streams. Cross-lane
/// parallelism (distinct providers) is never bounded here — only by `--jobs`.
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
    /// default 1). Acquired around every stream: one permit for a single-provider
    /// stream, a sorted permit set for a multi-source fan-out. Bounds only
    /// per-provider concurrency; `--jobs` still bounds cross-lane parallelism.
    lane_cap: LaneStreamCap,
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
    async fn try_multi_source(
        &self,
        order: &fetch::ResolvedTargets,
        hash: [u8; 32],
        staging: &Path,
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
        fetch::try_multi_source_fetch_from_admitted(
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
    async fn fetch_to_staging(
        &self,
        hash: [u8; 32],
        staging: &Path,
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
        match self.try_multi_source(&order, hash, staging, progress).await {
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

        let order = order.candidates;

        let mut last_err: Option<anyhow::Error> = None;
        for (attempt, cand) in order.iter().enumerate() {
            let target = (cand.node_id, cand.eth_address);
            // Registry multiaddrs as direct-address hints for a relay-free dial
            // to a reachable node (ADR 001 § Node Discovery).
            let dial_addrs = cand.dial_addrs();
            // The failover walk streams from one provider per attempt, so it holds
            // just that provider's lane-stream permit for the attempt, released
            // before the next candidate.
            let _lane_permit = self.lane_cap.permit(cand.eth_address).await?;
            let err = match self
                .fetch_to_staging_from(hash, target, &dial_addrs, staging, progress)
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => err,
            };
            let more = attempt + 1 < order.len();
            if retry_disposition(&err) == RetryDisposition::Terminal || !more {
                return Err(err);
            }
            tracing::warn!(
                "bundle pull: provider {} could not deliver an entry ({err:#}); failing over to \
                 the next of {} candidate(s)",
                cand.eth_address,
                order.len(),
            );
            last_err = Some(err);
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no candidate node could deliver the entry")))
    }

    /// Build one entry's ready-to-drive [`PoolContext`] and dial target for
    /// `provider`, shared by the whole-blob ([`Self::fetch_to_staging_from`]) and
    /// ranged ([`Self::drive_ranges_from`]) drive paths.
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

    /// Drive the given byte `ranges` of `hash` directly from `node_id`/`provider`
    /// into `staging`'s `.partial`, returning the ranged store. The ranged twin of
    /// [`Self::fetch_to_staging_from`]: builds the same context and target, then calls
    /// [`fetch::drive_ranges`] (which bao-verifies each range against `hash`,
    /// leaves the `.partial` unfinalized for the caller's splice-and-promote, and
    /// persists the lane's voucher watermark so a resume never re-pays).
    async fn drive_ranges_from(
        &self,
        hash: [u8; 32],
        fetch_target: FetchTarget,
        dial_addrs: &[std::net::SocketAddr],
        staging: &Path,
        ranges: &[(u64, u64)],
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<ClientRangedStore> {
        let provider = fetch_target.1;
        let (ctx, target) = self.build_pull_ctx(fetch_target, dial_addrs).await?;

        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let deps = self.drive_deps(max_blob_bytes)?;
        let pool_id = ctx.pool_id;

        let result = fetch::drive_ranges(
            &deps,
            ctx,
            target,
            provider,
            pool_id,
            hash,
            staging,
            ranges,
            Some(&self.ledgers),
            progress,
        )
        .await;
        match (result, self.grant.is_some()) {
            (Ok(store), _) => Ok(store),
            (Err(err), true) => Err(fetch::annotate_delegated_exhaustion(err)),
            (Err(err), false) => Err(err),
        }
    }

    /// Resolve the provider order for one dedup entry's range drives ONCE — the
    /// pinned `--node-id`, or a single [`fetch::probe_and_order`] over the
    /// discovery candidates. [`Self::pull_entry`] resolves this before its first
    /// sub-drive and threads it into every one (the complement drive, a donor
    /// re-fetch, and any whole-blob re-drive), so a self-heal entry probes the
    /// candidate set once rather than up to three times.
    async fn resolve_range_targets(&self, hash: [u8; 32]) -> anyhow::Result<RangeTargets> {
        if let Some(pinned) = self.explicit {
            return Ok(RangeTargets::Pinned(pinned));
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
        .await?
        .candidates;
        Ok(RangeTargets::Discovered(order))
    }

    /// Drive `ranges` of `hash` into `staging`'s `.partial` over a PRE-RESOLVED
    /// provider order (from [`Self::resolve_range_targets`]) — the pinned
    /// `--node-id`, or the probed discovery order walked with single-source
    /// failover (a retryable failure advances to the next candidate). Re-dials the
    /// resolved candidates without re-probing, so repeated sub-drives of one entry
    /// share a single probe round. The caller already holds a
    /// [`gate`](PullCtx::gate) permit, so this does not take one itself.
    ///
    /// Unlike [`Self::fetch_to_staging`] there is no multi-source fan-out here: the
    /// range-dedup path is an optimization over one source, and every driven range
    /// is still bao-verified against `hash`, so a single lane stays sound.
    async fn drive_ranges_ordered(
        &self,
        targets: &RangeTargets,
        hash: [u8; 32],
        staging: &Path,
        ranges: &[(u64, u64)],
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<ClientRangedStore> {
        let order = match targets {
            RangeTargets::Pinned(pinned) => {
                return self
                    .drive_ranges_from(hash, *pinned, &[], staging, ranges, progress)
                    .await;
            }
            RangeTargets::Discovered(order) => order,
        };

        let mut last_err: Option<anyhow::Error> = None;
        for (attempt, cand) in order.iter().enumerate() {
            let target = (cand.node_id, cand.eth_address);
            let dial_addrs = cand.dial_addrs();
            let err = match self
                .drive_ranges_from(hash, target, &dial_addrs, staging, ranges, progress)
                .await
            {
                Ok(store) => return Ok(store),
                Err(err) => err,
            };
            let more = attempt + 1 < order.len();
            if retry_disposition(&err) == RetryDisposition::Terminal
                || err.downcast_ref::<PoolExhausted>().is_some()
                || !more
            {
                return Err(err);
            }
            tracing::warn!(
                "bundle pull: provider {} could not deliver an entry's ranges ({err:#}); failing \
                 over to the next of {} candidate(s)",
                cand.eth_address,
                order.len(),
            );
            last_err = Some(err);
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no candidate node could deliver the entry ranges")))
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
        self.fetch_to_staging(hash, &staging, None).await?;
        let bytes =
            std::fs::read(&staging).with_context(|| format!("read {}", staging.display()))?;
        remove_staging(&staging);
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
    ) -> (Vec<EntryOutcome>, Transfer) {
        // Every entry declares an authoritative whole-file `hash`, so the by-hash
        // grouping path (fetch-once + link-duplicates, #1306) covers plain and
        // hint-carrying entries alike — the chunk hints only change HOW a group's
        // one blob is assembled, never that it is one paid unit per distinct hash.
        let index = ChunkIndex::default();
        let refs: Vec<&ManifestEntry> = entries.iter().collect();
        let (outcomes, transfer) = self.pull_plain(&refs, out_root, overwrite, &index).await;

        // A donor entry's finalized staging blob is the source a recipient splices
        // from, so it is kept past its own group's cleanup. With the run over,
        // every registered donor source is safe to remove — EXCEPT one a
        // `mark_retained` flagged: its blob is paid for but a destination failed to
        // materialize, so the finalized `<hex>` is the resume prefix a rerun needs
        // (deleting it would force a full re-fetch and re-payment).
        //
        // Disk cost: `materialize` copies rather than hard-links, so every
        // hint-carrying entry keeps its finalized staging blob here until this
        // sweep, on top of the materialized output. Peak disk for an optimized
        // bundle is therefore about output + staging (~2× the bundle size) — parity
        // with the prior chunked path. A follow-up can hard-link the first
        // materialize so the staging blob shares storage with its output.
        sweep_donor_sources(&index);
        (outcomes, transfer)
    }

    /// Fetch every distinct blob once (grouped by hash) and materialize it at each
    /// destination path (#1306), routing each group through [`Self::pull_entry`] so a
    /// blob whose chunk hints overlap an already-materialized sibling pays only for
    /// the complement. Live per-group bars are bounded at `--jobs`; `PullCtx.gate`
    /// is the real cap on in-flight fetches.
    async fn pull_plain(
        &self,
        entries: &[&ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        index: &ChunkIndex,
    ) -> (Vec<EntryOutcome>, Transfer) {
        let groups_by_hash = group_by_hash(entries);
        let group_count = groups_by_hash.len().max(1);
        let groups: Vec<Vec<EntryOutcome>> = futures_util::stream::iter(groups_by_hash)
            .map(|group| self.fetch_group(group, out_root, overwrite, index))
            // Live-bar fan-out bounded by --jobs; the global gate is the real in-flight-fetch cap.
            .buffer_unordered(self.jobs.min(group_count))
            .collect::<Vec<Vec<EntryOutcome>>>()
            .await;
        // Byte tally is per-group (a blob pulled once, materialized to N paths),
        // so sum it before flattening away the group boundaries.
        let transfer = groups
            .iter()
            .map(|g| group_transfer(g))
            .fold(Transfer::default(), Transfer::add);
        let outcomes = groups.into_iter().flatten().collect();
        (outcomes, transfer)
    }

    /// Reconstruct one entry's blob into `staging` (the finalized per-hash staging
    /// file [`Self::fetch_group`] then materializes to each destination), using chunk
    /// hints to dedup byte ranges against the run's [`ChunkIndex`] when they help.
    ///
    /// - No hints (or `total` unknown, or the blob is already finalized at
    ///   `staging`), or no chunk overlaps a materialized sibling → the plain
    ///   whole-file [`Self::fetch_to_staging`] path.
    /// - Otherwise the dedup path: pay to [`Self::drive_ranges_ordered`] only the
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
    /// can splice from this blob.
    async fn pull_entry(
        &self,
        hash: [u8; 32],
        hints: Option<&[Hint]>,
        total: Option<u64>,
        staging: &Path,
        index: &ChunkIndex,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<()> {
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
                return Ok(());
            }
            remove_staging(staging);
        }

        // Plan the dedup only when there is something to dedup against: hints and a
        // known total. The finalized-staging fast path above already returned, so
        // `staging` does not exist here.
        let plan = match (hints, total) {
            (Some(hints), Some(total)) if !hints.is_empty() => {
                let guard = index.map.lock().unwrap_or_else(PoisonError::into_inner);
                let plan = plan_dedup(hints, &guard, total);
                drop(guard);
                (!plan.donor.is_empty()).then_some((plan, total))
            }
            _ => None,
        };

        let Some((plan, total)) = plan else {
            // No donor overlap — pay for the whole file, then register its chunks
            // so a *later* entry can dedup against it.
            self.fetch_to_staging(hash, staging, progress).await?;
            index.register(hints, staging);
            return Ok(());
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
        let targets = self.resolve_range_targets(hash).await?;
        // Hold one lane-stream permit per provider this entry may drive from across
        // every sub-drive (complement, donor re-fetch, whole-blob re-drive) and the
        // splice between them — one logical fetch unit — so a co-entry never opens a
        // concurrent stream to a shared lane mid-reassembly. Sorted `Address` order
        // keeps it deadlock-free against a fan-out entry's permit set.
        let _lane_permits = self.lane_cap.permit_set(&targets.providers()).await?;
        let driver = CtxRangeDriver {
            ctx: self,
            targets: &targets,
            hash,
            staging,
            progress,
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

        let outcome =
            reassemble_dedup(&driver, &plan, total, hints, index, &finish_progress).await?;
        self.dedup_stats
            .spliced_bytes
            .fetch_add(outcome.spliced_bytes, Ordering::Relaxed);
        self.dedup_stats
            .hints_ignored
            .fetch_add(outcome.hints_ignored, Ordering::Relaxed);
        Ok(())
    }

    /// Fetch the blob shared by one hash-group and write it under `out_root` at
    /// each entry's path. The blob is fetched — and paid for — **once**; the first
    /// writable destination receives the materialized bytes and every other is a
    /// hard link (or copy) of it (#1306). Returns one [`EntryOutcome`] per input
    /// entry, in order. Never panics or short-circuits — every failure becomes an
    /// [`EntryOutcome::Failed`].
    async fn fetch_group(
        &self,
        group: HashGroup<'_>,
        out_root: &Path,
        overwrite: bool,
        index: &ChunkIndex,
    ) -> Vec<EntryOutcome> {
        // The group's shared hash is carried explicitly; parse it once, and a bad
        // hash fails every path in the group.
        let hash = match fetch::parse_hash(group.hash) {
            Ok(h) => h,
            Err(e) => {
                return group
                    .entries
                    .iter()
                    .map(|en| EntryOutcome::failed(&en.path, &e))
                    .collect();
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

        let slots = plan_slots(&group.entries, out_root, overwrite);

        // Every destination already present (or failed to resolve) → no fetch, no
        // payment. This is the whole point of the group: a duplicate path that is
        // already on disk costs nothing. The group's content is still part of the
        // whole-download total, so credit it straight to the total bar — no fetch
        // callback will, and the total would otherwise never reach 100%.
        if !slots.iter().any(|s| matches!(s, Slot::Write { .. })) {
            self.progress
                .credit_skipped(group.entries.iter().find_map(|e| e.size));
            return slots
                .into_iter()
                .map(|s| match s {
                    Slot::Failed(o) => o,
                    _ => EntryOutcome::Skipped,
                })
                .collect();
        }

        // Staged once per group: `drive_fetch` finalizes to
        // `out_root/.decdn-partial/<hex>` (#1497), streaming its `<hex>.partial`
        // + sidecars there — rather than buffering the blob — which is what lets a
        // large entry top up mid-fetch. The staging path derived from `out_root`
        // is only created once a group actually has something to write, never for
        // an all-skipped group.
        let staging = match staging_path(out_root, hash) {
            Ok(p) => p,
            Err(e) => return fail_all(slots, &e),
        };

        // One per-file bar for this group's single pull, labeled by its
        // destination path(s) — a fetch-once hash group shows one bar for every
        // path it lands at. The manifest's content size (all entries share a blob,
        // so one size) seeds the bar length as a pre-byte estimate; the first
        // delivered chunk replaces it with the authoritative wire length.
        let paths: Vec<String> = group.entries.iter().map(|e| e.path.clone()).collect();
        let size_estimate = group.entries.iter().find_map(|e| e.size);
        let file_bar = self
            .progress
            .file_bar(pull_progress::file_label(&paths), size_estimate);
        let fetched = self
            .pull_entry(
                hash,
                hints.as_deref(),
                total,
                &staging,
                index,
                file_bar.callback(),
            )
            .await;
        file_bar.finish();
        if let Err(e) = fetched {
            // `pull_entry` (whole-file or dedup) leaves the `<hex>.partial` +
            // `.obao4`/`.ranges` sidecars in place on error — they are what the
            // next run resumes from rather than re-paying for bytes already landed
            // (same contract as `fetch`'s `<output>.partial` store).
            return fail_all(slots, &e);
        }

        // `materialize` reads `staging` (not moved), so a retry after a failed
        // first write may call it again for the next writable path.
        let outcomes = materialize_group(
            slots,
            |dest| std::future::ready(materialize(&staging, &dest)),
            link_or_copy_atomic,
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
        // the copy-based fan-out must gate it.
        //
        // A donor blob (`is_donor`) is a splice source a later entry may still
        // read, so it is kept here regardless and swept once at run end by
        // `pull_all`.
        let any_failed = outcomes
            .iter()
            .any(|o| matches!(o, EntryOutcome::Failed { .. }));
        if !is_donor && !any_failed {
            // A non-donor whose every destination landed: its content is safely on
            // disk, so drop the staging blob now. A failed non-donor keeps its
            // `.partial` resume prefix; a donor is kept for splicing and swept at
            // run end.
            remove_staging(&staging);
        } else if is_donor && any_failed {
            // A donor whose blob is fully fetched and paid for but whose
            // materialize failed: retain its finalized `<hex>` from the run-end
            // sweep so a rerun resumes from it instead of re-paying the whole blob.
            index.mark_retained(&staging);
        }

        outcomes
    }
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
struct CtxRangeDriver<'a, P: Provider + Clone> {
    ctx: &'a PullCtx<'a, P>,
    targets: &'a RangeTargets,
    hash: [u8; 32],
    staging: &'a Path,
    progress: Option<&'a ProgressCallback>,
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
        Box::pin(async move {
            self.ctx
                .drive_ranges_ordered(self.targets, self.hash, self.staging, ranges, self.progress)
                .await
                .map(|_store| ())
        })
    }
}

/// Reassemble one dedup entry's blob into `staging`: drive the complement, splice
/// the donor ranges from disk, verify the whole-file BLAKE3, and promote — using
/// `driver` for every paid range drive.
///
/// The authoritative gate throughout is the whole-file BLAKE3; a bad or lying hint
/// only ever costs a re-download, never a corrupt output or a spuriously-failed
/// entry. After EVERY range drive (the complement, a donor re-fetch, and the
/// whole-blob re-drive) the store may have completed — `drive` then finalized and
/// renamed `<hex>.partial` -> `<hex>`. Each such point checks `staging.try_exists()`
/// and returns success rather than opening a `.partial` that no longer exists: a
/// resumed run whose prior `.partial` already held the donor-overlap ranges hits
/// this on the very FIRST complement drive.
///
/// `finish_progress` trues the file + total bars up to 100% on success: donor
/// bytes are spliced from disk and never flow through `drive`'s progress callback,
/// so a mostly-spliced entry would otherwise leave its bars short.
///
/// Returns the entry's [`DedupOutcome`] — bytes actually spliced from disk and
/// hints dropped by a fault — for the run-level report. A resume that finalized on
/// the first complement drive spliced nothing this run; a self-heal whole-blob
/// re-drive discards every splice, so it reports zero spliced bytes and counts all
/// its donors as ignored.
async fn reassemble_dedup(
    driver: &dyn RangeDriver,
    plan: &DedupPlan,
    total: u64,
    hints: Option<&[Hint]>,
    index: &ChunkIndex,
    finish_progress: &dyn Fn(),
) -> anyhow::Result<DedupOutcome> {
    let hash = driver.hash();
    let staging = driver.staging();
    let donor_total = plan
        .donor
        .iter()
        .map(|d| d.aligned.1)
        .fold(0u64, u64::saturating_add);

    // Pay only for the bytes no donor covers.
    driver.drive(&plan.complement).await?;

    // A resumed run may already hold the donor-overlap bytes in `.partial`, so this
    // first complement drive can COMPLETE the store — `drive` then ran its
    // whole-blob bao sweep against `hash` and renamed `<hex>.partial` -> `<hex>`.
    // The blob is finalized and verified; splicing would open a `.partial` that no
    // longer exists. Register the donor chunks and return. No splice ran this run,
    // so nothing is reported spliced.
    if staging.try_exists()? {
        finish_progress();
        index.register(hints, staging);
        return Ok(DedupOutcome::default());
    }

    let partial = partial_path(staging);

    // Verify + splice each donor range off the executor. A donor whose chunk no
    // longer hashes to its hint (a lying donor hint, or a short read) is re-fetched
    // normally rather than trusted.
    let donors = plan.donor.clone();
    let partial_for_splice = partial.clone();
    let refetch: Vec<(u64, u64)> =
        tokio::task::spawn_blocking(move || splice_donors(&partial_for_splice, &donors))
            .await
            .map_err(|e| anyhow!("donor splice task: {e}"))??;
    // Bytes served from a verified donor splice are `donor_total` minus what had to
    // be re-fetched; each re-fetched donor is a dropped hint.
    let refetch_total = refetch.iter().map(|r| r.1).fold(0u64, u64::saturating_add);
    let mut spliced_bytes = donor_total.saturating_sub(refetch_total);
    let mut hints_ignored = u64::try_from(refetch.len()).unwrap_or(u64::MAX);
    if !refetch.is_empty() {
        driver.drive(&refetch).await?;
        // If every donor was untrusted, `refetch` is the whole donor set, so the
        // driven complement plus this re-fetch cover the whole blob: `drive` then
        // ran its whole-blob bao sweep against `hash` and renamed `.partial` ->
        // `staging`. The blob is finalized and verified — do not hash/promote a
        // `.partial` that no longer exists; register the donor chunks and return.
        if staging.try_exists()? {
            finish_progress();
            index.register(hints, staging);
            return Ok(DedupOutcome {
                spliced_bytes,
                hints_ignored,
            });
        }
    }

    // The authoritative check: the whole reassembled blob must hash to `hash`.
    let partial_for_hash = partial.clone();
    let got = tokio::task::spawn_blocking(move || hash_partial(&partial_for_hash))
        .await
        .map_err(|e| anyhow!("whole-file hash task: {e}"))??;
    if got != hash {
        // A lying recipient hint placed a chunk at the wrong offset. Drop the
        // spliced ranges by re-driving the whole blob (the ranged store fetches
        // exactly the bytes the splice wrote, bao-verified against `hash`) and
        // re-verify. Every donor is discarded, so nothing was saved and all of them
        // count as ignored.
        spliced_bytes = 0;
        hints_ignored = u64::try_from(plan.donor.len()).unwrap_or(u64::MAX);
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

/// The run-end sweep of donor staging blobs: remove every registered donor
/// source EXCEPT one [`ChunkIndex::mark_retained`] flagged (its blob is paid for
/// but a destination failed to materialize, so its finalized `<hex>` is the resume
/// prefix a rerun needs — deleting it would force a full re-fetch and re-payment).
fn sweep_donor_sources(index: &ChunkIndex) {
    for source in index.sources() {
        if !index.retained(&source) {
            remove_staging(&source);
        }
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
        let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        for h in hints {
            map.entry(h.hash).or_insert_with(|| MaterializedRange {
                source: source.to_path_buf(),
                offset: h.offset,
                len: h.len,
            });
        }
    }

    /// The distinct donor staging blobs registered this run, for the run-end
    /// sweep in [`PullCtx::pull_all`].
    fn sources(&self) -> Vec<PathBuf> {
        let map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        let mut seen: HashSet<&Path> = HashSet::new();
        let mut out = Vec::new();
        for m in map.values() {
            if seen.insert(m.source.as_path()) {
                out.push(m.source.clone());
            }
        }
        out
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

/// One splice source for the dedup path: a chunk-group-aligned run of the
/// recipient blob whose bytes a materialized donor already holds.
#[derive(Debug, Clone)]
struct DonorRange {
    /// `(offset, len)` in the recipient blob to write — chunk-group-aligned, so
    /// the ranged store's complement (fetched on group boundaries) and this
    /// splice tile the blob without overlap.
    aligned: (u64, u64),
    /// The donor blob to read from.
    source: PathBuf,
    /// Source offset of the aligned subset within `source`.
    src_offset: u64,
    /// The whole chunk's hash — the aligned subset is trusted only after the
    /// whole chunk at `[chunk_src_offset, chunk_src_offset + chunk_len)` in
    /// `source` re-hashes to this.
    chunk_hash: [u8; 32],
    /// Whole chunk start in `source` (for the verification re-hash).
    chunk_src_offset: u64,
    /// Whole chunk length in bytes (for the verification re-hash).
    chunk_len: u64,
}

/// The dedup plan for one entry: donor ranges to splice from disk and the
/// complement ranges to pay for.
struct DedupPlan {
    /// Group-aligned ranges a materialized sibling already holds.
    donor: Vec<DonorRange>,
    /// The `(offset, len)` runs no donor covers — the bytes to drive and pay for.
    complement: Vec<(u64, u64)>,
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

/// Plan the range-dedup for one entry against the run's chunk `index`.
///
/// For each hint whose chunk is already materialized, `[offset, offset + len)` is
/// aligned INWARD to 16 KiB chunk groups — only whole groups fully inside the hint
/// become a donor range, and the ≤1 partial group at each edge falls into the
/// complement (the ranged store bao-verifies on group boundaries, so a donor
/// splice must land on them). A hint smaller than one group, or unaligned so no
/// whole group fits, contributes no donor range. The complement is `total` minus
/// the union of the donor ranges — the bytes that must still be paid for.
fn plan_dedup(
    hints: &[Hint],
    index: &HashMap<[u8; 32], MaterializedRange>,
    total: u64,
) -> DedupPlan {
    let group = CHUNK_GROUP_BYTES;
    let mut donor = Vec::new();
    let mut donor_union: Vec<(u64, u64)> = Vec::new();
    for h in hints {
        let Some(m) = index.get(&h.hash) else {
            continue;
        };
        // The same chunk hash but a different claimed length: one side's manifest
        // lies about this chunk. Don't dedup it — leave the hint's range in the
        // complement (paid for and bao-verified) instead of trusting a placement
        // that would run past the donor's real chunk end.
        if m.len != h.len {
            continue;
        }
        let hint_end = h.offset.saturating_add(h.len);
        // Inward alignment: first group boundary at or after `offset`, last group
        // boundary at or before `hint_end`.
        let g_start = h.offset.div_ceil(group).saturating_mul(group);
        let g_end = (hint_end / group) * group;
        if g_start >= g_end {
            continue;
        }
        let alen = g_end - g_start;
        let src_offset = m.offset.saturating_add(g_start - h.offset);
        donor.push(DonorRange {
            aligned: (g_start, alen),
            source: m.source.clone(),
            src_offset,
            chunk_hash: h.hash,
            chunk_src_offset: m.offset,
            chunk_len: m.len,
        });
        donor_union.push((g_start, alen));
    }
    DedupPlan {
        complement: complement_runs(&donor_union, total),
        donor,
    }
}

/// The `.partial` data file the ranged store keeps beside a finalized `staging`
/// blob — `<hex>.partial` — where driven ranges and spliced donor bytes land
/// before promotion.
fn partial_path(staging: &Path) -> PathBuf {
    staging.with_extension("partial")
}

/// Verify and splice every donor range into `partial`, returning the aligned
/// ranges that could NOT be trusted (a donor whose chunk no longer hashes to its
/// hint, or an unreadable source) and so must be re-fetched and paid for.
///
/// A donor's bytes are trusted only after the WHOLE chunk at its recorded source
/// offset re-hashes to the chunk hash — a donor's own chunk placement is an
/// unverified manifest claim until then. The trusted group-aligned subset is then
/// written at the recipient offset; the whole-file BLAKE3 the caller runs next is
/// the authoritative backstop against a lying recipient placement.
fn splice_donors(partial: &Path, donors: &[DonorRange]) -> anyhow::Result<Vec<(u64, u64)>> {
    use std::io::{Seek, SeekFrom};

    let mut refetch = Vec::new();
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .open(partial)
        .with_context(|| format!("open {}", partial.display()))?;
    for d in donors {
        let (dst_offset, len) = d.aligned;
        // An unreadable donor source is not trusted — pay to fetch it.
        let Ok(mut src) = std::fs::File::open(&d.source) else {
            refetch.push(d.aligned);
            continue;
        };
        if !chunk_verified(&mut src, d.chunk_src_offset, d.chunk_len, d.chunk_hash) {
            refetch.push(d.aligned);
            continue;
        }
        // The chunk is confirmed present at `chunk_src_offset`; copy its
        // group-aligned subset into the recipient's `.partial`. A seek/copy error
        // here (a truncated or racing donor file, an I/O fault) is not fatal: the
        // donor bytes are simply not trusted, so queue the aligned range for a
        // paid, bao-verified re-fetch that overwrites exactly it — nothing torn by
        // a partial copy survives, and the whole-file BLAKE3 the caller runs next
        // is the authoritative backstop.
        let copied = src
            .seek(SeekFrom::Start(d.src_offset))
            .and_then(|_| out.seek(SeekFrom::Start(dst_offset)))
            .and_then(|_| copy_exact(&mut src, &mut out, len));
        if copied.is_err() {
            refetch.push(d.aligned);
        }
    }
    out.sync_all()
        .with_context(|| format!("sync {}", partial.display()))?;
    Ok(refetch)
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
    use std::io::Read;
    let mut f =
        std::fs::File::open(partial).with_context(|| format!("open {}", partial.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
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

    // Clamp each donor range to `[0, total)` and drop empty/out-of-range ones,
    // then sort by start so overlapping/adjacent runs coalesce in one pass.
    let mut runs: Vec<(u64, u64)> = donor_aligned
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

/// Report an empty would-fetch set. `by_filter` is true only when a non-empty
/// bundle was emptied by `--include`/`--exclude`, so the operator learns their
/// globs matched nothing rather than mistaking it for an empty bundle.
fn report_nothing_to_fetch(by_filter: bool) {
    if by_filter {
        println!("no bundle entries match the include/exclude filters; nothing to fetch");
    } else {
        println!("bundle has no entries; nothing to fetch");
    }
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
                report_nothing_to_fetch(filters_given && !raw_empty);
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
    for o in outcomes {
        match o {
            EntryOutcome::Fetched(_) => fetched += 1,
            EntryOutcome::Linked => linked += 1,
            EntryOutcome::Skipped => skipped += 1,
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
    };
    if json {
        let line = serde_json::to_string(&rep).map_err(|e| anyhow!("serialize report: {e}"))?;
        println!("{line}");
    } else {
        println!(
            "pulled into {} ({fetched} fetched, {linked} linked, {skipped} skipped, \
             {failed} failed)",
            output.display()
        );
        // `downloaded X → reconstructed Y` only when dedup made them differ;
        // otherwise a single `downloaded X`.
        println!("{}", transfer_line(transfer));
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

/// The [`Transfer`] for one whole-file hash-group: the blob is pulled once
/// (`downloaded` = its size, taken from the single `Fetched`), and every
/// materialized copy — the `Fetched` plus each `Linked` duplicate path — is a
/// full file on disk (`reconstructed` = size × copies). A group with nothing
/// written (all skipped or failed) contributes nothing.
fn group_transfer(outcomes: &[EntryOutcome]) -> Transfer {
    let file_size = outcomes.iter().find_map(|o| match o {
        EntryOutcome::Fetched(n) => Some(*n),
        _ => None,
    });
    match file_size {
        Some(n) => {
            let copies = outcomes
                .iter()
                .filter(|o| matches!(o, EntryOutcome::Fetched(_) | EntryOutcome::Linked))
                .count();
            let copies = u64::try_from(copies).unwrap_or(u64::MAX);
            Transfer {
                downloaded: n,
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
        let plan = plan_dedup(&hints, &index, total);

        assert_eq!(plan.donor.len(), 1);
        let d = &plan.donor[0];
        assert_eq!(d.aligned, (GROUP, GROUP));
        assert_eq!(d.source, PathBuf::from("/tmp/donorA"));
        assert_eq!(d.src_offset, 0);
        assert_eq!(d.chunk_hash, h);
        assert_eq!((d.chunk_src_offset, d.chunk_len), (0, GROUP));
        assert_eq!(plan.complement, vec![(0, GROUP), (2 * GROUP, GROUP)]);
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
        let plan = plan_dedup(&hints, &index, total);

        assert_eq!(plan.donor.len(), 1);
        let d = &plan.donor[0];
        // The span [100, 100 + 2*GROUP + 500) straddles boundaries GROUP and
        // 2*GROUP, so exactly ONE whole group — [GROUP, 2*GROUP) — is inside it;
        // the sub-group head and tail fall into the complement.
        assert_eq!(d.aligned, (GROUP, GROUP));
        // Source offset shifts by (GROUP - 100) from the chunk's donor start.
        assert_eq!(d.src_offset, 5000 + (GROUP - 100));
        assert_eq!(plan.complement, vec![(0, GROUP), (2 * GROUP, 2 * GROUP)]);
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
        let plan = plan_dedup(&hints, &HashMap::new(), total);
        assert!(plan.donor.is_empty());
        assert_eq!(plan.complement, vec![(0, total)]);
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
        let plan = plan_dedup(&hints, &index, total);
        assert!(plan.donor.is_empty());
        assert_eq!(plan.complement, vec![(0, total)]);
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
        let plan = plan_dedup(&hints, &index, total);
        assert!(
            plan.donor.is_empty(),
            "a length-mismatched donor must not be spliced"
        );
        assert_eq!(plan.complement, vec![(0, total)]);
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
            aligned: (GROUP, 2 * GROUP),
            source: donor_path,
            src_offset: 0,
            chunk_hash,
            chunk_src_offset: 0,
            chunk_len: GROUP,
        };

        let refetch = splice_donors(&partial, std::slice::from_ref(&donor))
            .expect("a copy that EOFs must not fail the splice");
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
        // A dedup plan with a real donor (the path is never read — the guard
        // returns before any splice).
        let plan = DedupPlan {
            donor: vec![DonorRange {
                aligned: (GROUP, GROUP),
                source: tmp.path().join("donor-never-read"),
                src_offset: 0,
                chunk_hash: [0x11; 32],
                chunk_src_offset: 0,
                chunk_len: GROUP,
            }],
            complement: vec![(0, GROUP)],
        };
        let index = ChunkIndex::default();

        let res = reassemble_dedup(&driver, &plan, 2 * GROUP, None, &index, &|| {}).await;

        assert!(
            res.is_ok(),
            "a first complement drive that finalizes the blob must succeed, not \
             fail on a missing .partial: {res:?}"
        );
        assert_eq!(driver.drives.get(), 1, "only the complement drive ran");
        assert!(staging.try_exists().expect("stat staging"));
    }

    /// Review #3 (money-band): the run-end sweep removes a normal donor staging
    /// blob but KEEPS one `mark_retained` flagged (its group failed to
    /// materialize), so a rerun resumes from the finalized `<hex>` instead of
    /// re-paying the whole blob.
    #[test]
    fn run_end_sweep_keeps_a_retained_donor_and_removes_a_normal_one() {
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

        sweep_donor_sources(&index);

        assert!(
            !normal.exists(),
            "a normal donor source is swept at run end"
        );
        assert!(
            retained.exists(),
            "a retained (materialize-failed) donor source survives the sweep"
        );
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
            aligned: (GROUP, GROUP),
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
            aligned: (GROUP, GROUP),
            source: donor_path,
            src_offset: 0,
            chunk_hash,
            chunk_src_offset: 0,
            chunk_len: GROUP,
        };

        let refetch = splice_donors(&partial, std::slice::from_ref(&donor)).expect("splice");
        assert_eq!(refetch, vec![donor.aligned]);

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
        let t = group_transfer(&outcomes);
        assert_eq!(t.downloaded, 100);
        assert_eq!(t.reconstructed, 300);
    }

    #[test]
    fn group_transfer_skips_and_fails_contribute_nothing() {
        let outcomes = vec![EntryOutcome::Fetched(100), EntryOutcome::Skipped];
        let t = group_transfer(&outcomes);
        assert_eq!(t.downloaded, 100);
        assert_eq!(t.reconstructed, 100);

        let none = vec![EntryOutcome::Failed {
            path: "x".into(),
            err: "boom".into(),
        }];
        let t = group_transfer(&none);
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

    /// Skip-existing / `--overwrite` are decided **per destination**: one path of a
    /// duplicated blob can be already-present (Skip) while its twin is absent
    /// (Write), and `--overwrite` forces both to Write.
    #[test]
    fn plan_slots_classifies_each_destination_independently() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path();
        std::fs::write(out.join("present.txt"), b"x").unwrap();
        let entries = [entry("present.txt", "b3:h"), entry("absent.txt", "b3:h")];
        let refs: Vec<&ManifestEntry> = entries.iter().collect();

        let slots = plan_slots(&refs, out, false);
        assert!(matches!(slots[0], Slot::Skip));
        assert!(matches!(slots[1], Slot::Write { .. }));

        let slots = plan_slots(&refs, out, true);
        assert!(matches!(slots[0], Slot::Write { .. }));
        assert!(matches!(slots[1], Slot::Write { .. }));
    }

    /// A path that escapes `out_root` is a per-destination failure, not a fetch.
    #[test]
    fn plan_slots_marks_unsafe_paths_failed() {
        let dir = tempfile::tempdir().unwrap();
        let entries = [entry("../escape", "b3:h")];
        let refs: Vec<&ManifestEntry> = entries.iter().collect();
        let slots = plan_slots(&refs, dir.path(), false);
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
        let slots = plan_slots(&refs, dir.path(), false);
        assert!(matches!(slots[0], Slot::Failed(_)));
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
                Ok(())
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
                Ok(())
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
