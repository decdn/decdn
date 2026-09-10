//! `decdn bundle pull` — fetch every blob a bundle manifest references into an
//! output directory over the paid `cdn/client/v1` path (issue #391).
//!
//! The manifest comes from a local file (`-i`) or is fetched first by its own
//! BLAKE3 hash (`--hash`); either way entries are then fetched per *distinct*
//! blob hash. Entries naming the same blob (one file at two paths) are fetched —
//! and paid for — once and hard-linked (or copied) to each path (#1306). Node selection is
//! per blob (#936): with an explicit `--node-id` every entry is pulled from that
//! one node, otherwise each distinct blob discovers its own holder among the
//! region-nearest active nodes. Distinct blobs are fetched with `--jobs`
//! concurrency.
//!
//! **One shared pool.** The whole bundle pulls from the caller's single
//! `PaymentPool` deposit (ADR 003) — opened once and reused across every
//! provider the manifest touches. Two concurrency guards follow from that: per-provider
//! async mutexes serialize voucher signing on each provider's lane (vouchers are
//! cumulative per `(signer, provider)` lane, so two in-flight fetches sharing
//! one lane would race it) — a multi-source entry (ADR 039) holds the locks for
//! its whole admitted provider set, acquired in one global order so overlapping
//! sets cannot deadlock — and a single global mutex serializes every
//! open-or-reuse call — the pool's on-chain state (deposit, allowance) is one
//! shared resource now, regardless of which provider an entry is bound for.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

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
use decdn_client_pull::discovery::{self, NodeCandidate};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::provider;
use decdn_client_pull::{
    PoolExhausted, ProgressCallback, PullDeadlines, RetryDisposition, retry_disposition,
};

type FetchTarget = (PublicKey, Address);

/// A shared, fetch-once cell for one chunk blob's result — its content size, or a
/// formatted fetch error. Resolved by the first chunked file to need the chunk and
/// reused by any other file that shares it, so a shared chunk is fetched and paid
/// for exactly once. A cached `Err` fails every dependent file without a re-fetch.
type ChunkCell = Arc<tokio::sync::OnceCell<Result<u64, String>>>;

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
    /// Optional ordered chunk decomposition (a dedup helper, per
    /// `appendix-bundles.md`). When present the file is fetched as the in-order
    /// concatenation of these chunk blobs and validated against the whole-file
    /// `hash`; when absent the file is fetched as one blob by `hash`.
    #[serde(default)]
    chunks: Option<Vec<ManifestChunk>>,
}

/// One chunk of a chunked [`ManifestEntry`]: an independently BLAKE3-addressed
/// blob. Only `hash` is read — the file is fetched chunk-by-chunk and validated
/// by the whole-file BLAKE3, so a chunk's informational `size` in the JSON is
/// accepted and ignored (serde drops the unknown field).
#[derive(Debug, Deserialize)]
struct ManifestChunk {
    hash: String,
}

/// The run's whole-download content size — the fixed denominator for the total
/// progress bar. Whole-file blobs are deduped by hash (a blob fetched once and
/// materialized to several paths counts once, matching the fetch-once grouping);
/// chunked files count once each. A blob whose manifest `size` is absent
/// contributes nothing, exactly as it then moves the total bar not at all, so the
/// numerator and denominator stay consistent. `None` when nothing kept declares a
/// size — the total bar is then omitted and only per-file bars render.
fn total_content_bytes(entries: &[ManifestEntry]) -> Option<u64> {
    // Whole-file entries keyed by hash, OR-ing in a declared size wherever one of
    // the same-hash entries carries it (the file bar picks its size the same way).
    let mut plain: HashMap<&str, Option<u64>> = HashMap::new();
    let mut sum: u64 = 0;
    let mut any_sized = false;
    for entry in entries {
        if entry.chunks.is_some() {
            if let Some(s) = entry.size {
                sum = sum.saturating_add(s);
                any_sized = true;
            }
        } else {
            let slot = plain.entry(entry.hash.as_str()).or_insert(None);
            *slot = slot.or(entry.size);
        }
    }
    for size in plain.values().flatten() {
        sum = sum.saturating_add(*size);
        any_sized = true;
    }
    any_sized.then_some(sum)
}

/// The compiled `--include`/`--exclude` globs that select which manifest entries
/// a pull run fetches. Both sets match an entry's POSIX relative `path` — the
/// manifest field (`models/a.bin`), never the on-disk absolute path — with the
/// same gitignore glob dialect `bundle create --exclude` uses (`*` does not
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
/// counts; `downloaded` is the distinct content bytes fetched (each shared chunk
/// or duplicated blob counted once) and `reconstructed` is the total bytes
/// written to disk this run — they diverge exactly when dedup saved a transfer.
/// `downloaded` is a content-size tally, not an exact on-wire measurement: it
/// excludes bao proof overhead and still counts a chunk served from an existing
/// staging file on a resumed run.
#[derive(Serialize)]
struct PullReport {
    output: String,
    fetched: u64,
    linked: u64,
    skipped: u64,
    failed: u64,
    downloaded: u64,
    reconstructed: u64,
}

/// A pull's byte accounting: `downloaded` is the distinct content bytes fetched
/// (a chunk or blob shared across entries counts once); `reconstructed` is the
/// total bytes written to disk (every materialized copy). The two are equal
/// unless dedup — shared chunks, or a blob at several paths — let one fetch serve
/// several files. `downloaded` sums content lengths, not exact on-wire bytes: it
/// omits bao proof overhead and still counts a chunk resumed from staging.
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
        locks: LaneLocks::default(),
        open_lock: tokio::sync::Mutex::new(()),
        pool_serial: tokio::sync::Mutex::new(()),
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
        .pull_all(
            &manifest.entries,
            &args.output,
            args.overwrite,
            args.jobs.max(1),
        )
        .await;
    ctx.progress.finish();

    report(&outcomes, transfer, &args.output, args.json)
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

/// The bundle's per-provider lane locks. A `(signer, provider)` voucher lane is
/// cumulative, so two in-flight fetches sharing one lane would race its
/// watermark; the lock serializes them. Locks are lazily created per provider
/// and held across one entry's fetch.
///
/// A single-source entry takes the one lock for its provider; a multi-source
/// entry (ADR 039, #1774) takes the locks for its whole admitted provider set
/// via [`Self::lock_set`], acquired in one global order — sorted by `Address`,
/// deduplicated. Consistent ordering is what keeps two concurrent entries with
/// overlapping provider sets deadlock-free: an entry only ever waits on the
/// lowest unacquired lock of its sorted set, so no hold-and-wait cycle can
/// form. The nesting also matches the single-source path's provider-lock →
/// `open_lock` order, so the two lock kinds cannot deadlock each other either.
///
/// The map is a `tokio::sync::Mutex`: `LaneLocks` is `Sync` so `PullCtx` is
/// `Sync` and safe to share across `tokio::spawn` if needed. The map guard is
/// held only to `entry` + `clone` the `Arc`, never across a provider-lock
/// await. The locks themselves are `Arc` because `OwnedMutexGuard` — which lets
/// a multi-source entry carry its whole lock-set in one `Vec` — needs it.
#[derive(Default)]
struct LaneLocks {
    map: tokio::sync::Mutex<HashMap<Address, Arc<tokio::sync::Mutex<()>>>>,
}

impl LaneLocks {
    /// The per-provider lane lock, created on first use.
    async fn lock(&self, provider: Address) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.map.lock().await;
        Arc::clone(
            map.entry(provider)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Acquire the lane locks for every provider in `providers`, in the global
    /// order, and return the guards in acquisition order.
    async fn lock_set(&self, providers: &[Address]) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        let mut ordered = providers.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        let mut guards = Vec::with_capacity(ordered.len());
        for provider in ordered {
            guards.push(self.lock(provider).await.lock_owned().await);
        }
        guards
    }
}

/// Shared, by-reference state for the entry fetch loop. Borrowed by every
/// in-flight entry future. `LaneLocks` is `Sync` via `tokio::sync::Mutex`, so
/// `PullCtx` is `Sync` and safe to share across `tokio::spawn` if needed;
/// `buffer_unordered` currently polls in one task.
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
    /// Per-provider lane locks (voucher-watermark serialization), see
    /// [`LaneLocks`].
    locks: LaneLocks,
    /// Serializes every pool open-or-reuse across the whole bundle. The
    /// bundle's every entry shares ONE `PaymentPool` deposit (ADR 003), so
    /// distinct providers cannot open concurrently: its on-chain state (deposit,
    /// standing USDC allowance) is one resource regardless of which provider an
    /// entry is bound for. Taken around the whole open-or-reuse call (which may
    /// also perform a low-water top-up) and released before streaming, so
    /// delivery itself still runs concurrently once each entry has its context.
    open_lock: tokio::sync::Mutex<()>,
    /// Serializes the pool's streaming phase across the whole bundle. Every
    /// entry shares ONE `PaymentPool` deposit, but the scheduler's `spent`
    /// view (`scheduler.rs:769`) sums only the lanes of the current fetch.
    /// Without this, two `--jobs` entries on disjoint provider sets have
    /// independent remaining-deposit views and can jointly over-issue vouchers
    /// that exceed the single deposit and cannot all redeem. Holding this
    /// across the whole payment section (`try_multi_source` + fallback) ensures
    /// only one entry drives at a time; probes remain concurrent and
    /// multi-source within an entry still fans out via its own `SharedPool`.
    pool_serial: tokio::sync::Mutex<()>,
    /// The run's multi-bar progress renderer: one per-file bar per active pull
    /// above a bottom total bar (silent off a terminal or under `--json`). Set
    /// once the kept manifest is known — its entries decide the total-bar mode —
    /// so it starts [disabled](PullProgress::disabled) during the manifest fetch.
    progress: PullProgress,
}

impl<P: Provider + Clone> PullCtx<'_, P> {
    /// The per-provider lane lock, created on first use.
    async fn provider_lock(&self, provider: Address) -> Arc<tokio::sync::Mutex<()>> {
        self.locks.lock(provider).await
    }

    /// Acquire the lane locks for every provider in `providers`, in one global
    /// order (see [`LaneLocks::lock_set`]).
    async fn provider_lock_set(
        &self,
        providers: &[Address],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        self.locks.lock_set(providers).await
    }

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
            // budget bounds both stages, under a cap that must outlast them both.
            deadlines: PullDeadlines::capped(
                self.common.stall_timeout(),
                self.common.stall_timeout(),
                self.common.min_throughput_bps(),
                self.common.hard_cap(),
            )?,
        })
    }

    /// The ADR 039 multi-source pre-branch for one entry (#1774). Returns
    /// `Ok(Some(()))` once the blob is fetched in parallel across the admitted
    /// set, `Ok(None)` when the engagement gate declines (the caller then runs
    /// the single-source failover loop), and `Err` when the fan-out engaged and
    /// failed.
    ///
    /// The lane locks for the whole admitted provider set are held across the
    /// fan-out — acquired in one global order so a concurrent entry with an
    /// overlapping provider set waits rather than deadlocks — and the bundle's
    /// `open_lock` is passed through so every lane's pool open-or-reuse still
    /// serializes against the other entries sharing the one on-chain pool. The
    /// pre-probe gate (kill switch, holder count, size-hint floor) runs BEFORE
    /// the lock-set: a fetch the gate declines never fans out, so taking the
    /// lanes would only block concurrent entries sharing those providers.
    ///
    /// Held across the whole streaming transfer (header probe + `multi_source_fetch`):
    /// vouchers are cumulative per `(signer, provider)` lane and the ledger arms
    /// the voucher before the ack, so releasing between intervals would race the
    /// watermark. This intentionally serializes concurrent entries sharing any
    /// provider for the full large-blob transfer — with `--jobs 3` and overlapping
    /// provider sets parallelism degrades to serial. Narrowing to the
    /// voucher-exchange critical section is future work if the ledger can be made
    /// per-interval (see ADR 039).
    async fn try_multi_source(
        &self,
        order: &fetch::ResolvedTargets,
        hash: [u8; 32],
        staging: &Path,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<Option<()>> {
        // Admission is computed once and reused for the gate, the lane-lock set,
        // and the fan-out itself — `try_multi_source_fetch` would otherwise
        // recompute the same `admit_sources` from `order.candidates`.
        let admitted = discovery::admit_sources(order.candidates.clone(), self.common.max_sources);
        if fetch::multi_source_gate_declines(
            self.common,
            &order.candidates,
            &admitted,
            order.size_hint,
        ) {
            return Ok(None);
        }
        let providers: Vec<Address> = admitted.iter().map(|c| c.eth_address).collect();
        let _guards = self.provider_lock_set(&providers).await;

        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        let deps = self.drive_deps(max_blob_bytes)?;
        // The entry's per-file bar callback (ADR 039 fan-out reports one monotonic
        // total the lanes fold their per-leg deltas into, so the bar never jumps
        // between lanes). The admitted set already computed for the gate and the
        // lock is moved into the fan-out so `admit_sources` runs only once.
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
        if let Some(pinned) = self.explicit {
            // A `--node-id`-pinned target takes its direct address from `--addr`,
            // not the registry, so no on-chain dial hints apply.
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

        // Pool-level serialization (see `Self::pool_serial`): every bundle entry
        // shares ONE `PaymentPool` deposit, but the scheduler's `spent` view
        // (`scheduler.rs:769`) sums only the lanes of the current fetch. Two
        // `--jobs` entries on disjoint provider sets would otherwise have
        // independent remaining-deposit views and could jointly over-issue
        // vouchers that exceed the single deposit and cannot all redeem. Probes
        // remain concurrent (above), but the payment + streaming section is
        // serialized here; multi-source within an entry still fans out via its
        // own `SharedPool`.
        let _pool_guard = self.pool_serial.lock().await;

        // Multi-source fan-out (ADR 039, #1774): the same pre-branch `decdn
        // fetch` runs. One entry engages N provider lanes at once, so it takes
        // the lane locks for its whole admitted set — in one global order, so
        // two entries with overlapping sets cannot deadlock — then fans out. The
        // engagement gate (kill switch off, too few admissible holders, below the
        // size floor) is decided inside `try_multi_source` BEFORE the locks are
        // taken and falls through to the single-source failover loop below
        // unchanged; a retryable fan-out failure does the same, resuming the
        // entry's `.partial` so nothing paid for is re-bought.
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

    /// Fetch directly from `node_id`/`provider`, bypassing discovery, streaming
    /// into `staging`.
    async fn fetch_to_staging_from(
        &self,
        hash: [u8; 32],
        (node_id, provider): FetchTarget,
        dial_addrs: &[std::net::SocketAddr],
        staging: &Path,
        progress: Option<&ProgressCallback>,
    ) -> anyhow::Result<()> {
        // Serialize all access to this provider's lane: the voucher-signing
        // critical section must be atomic per lane.
        let lock = self.provider_lock(provider).await;
        let _guard = lock.lock().await;

        // Owned and local to one entry's fetch: `drive_fetch` takes `ctx` by
        // value (it wraps it in `Arc<Mutex>` so its source and driver can share a
        // mid-fetch top-up's new deposit), so this binding just supplies it once
        // and moves it in below.
        // Delegated: adopt the pool + capability from the token (the whole
        // binding+capability context is built by the shared helper). Self-owned:
        // open-or-reuse the caller's pool under the global open lock, then attach
        // the ADR 005 client binding. Both yield a ready-to-drive context.
        // Delegated (`--capability`): every entry adopts the named pool + owner
        // capability (no open). Self-owned: open-or-reuse the caller's pool under
        // the global open lock, then attach the ADR 005 client binding.
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
            // Attach the ADR 005 client binding, exactly as
            // `fetch::build_pool_ctx` does on its open path. Without it the
            // request carries no verified buyer identity, so the node's
            // `pull_authorized` gate never fires a cache-miss origin pull and
            // `--namespace` would be inert here. Signed outside `open_lock` — it
            // touches no on-chain state.
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
        // caller (`fetch_group` / `pull_chunked`), so the finish hook here is a
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
    /// nor *paid for*, twice. `buffer_unordered` polls up to `jobs` futures in
    /// this one task (no `tokio::spawn`, by choice — nothing here is `!Send`);
    /// parallelism comes from concurrent in-flight network I/O, while the
    /// per-provider locks inside `fetch_to_staging_from` serialize same-lane
    /// access — now over unique blobs.
    async fn pull_all(
        &self,
        entries: &[ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        jobs: usize,
    ) -> (Vec<EntryOutcome>, Transfer) {
        // Whole-file entries take the by-hash grouping path (fetch-once +
        // link-duplicates, #1306); chunked entries take the concatenation path
        // (fetch each distinct chunk blob once, then assemble). The two sets are
        // disjoint by construction — an entry either has a `chunks` list or does
        // not — so they run independently and their outcomes concatenate.
        let plain: Vec<&ManifestEntry> = entries.iter().filter(|e| e.chunks.is_none()).collect();
        let chunked: Vec<&ManifestEntry> = entries.iter().filter(|e| e.chunks.is_some()).collect();

        // A blob can appear both as a whole-file entry `hash` and as a chunk of
        // another entry (a chunk also published standalone). The two phases share
        // one content-addressed staging dir, so the fix for "fetch/pay once" is to
        // stop the plain phase from deleting a staging blob the chunked phase still
        // needs: the chunked phase then resumes from the finalized blob and pays
        // nothing, and its own cleanup removes it. Parse errors drop out — an
        // unparseable chunk hash is never fetched and can't equal a valid plain one.
        let chunk_keep: HashSet<[u8; 32]> = chunked
            .iter()
            .filter_map(|e| parse_chunk_hashes(e).ok())
            .flatten()
            .collect();

        let (mut outcomes, plain_bytes) = self
            .pull_plain(&plain, out_root, overwrite, jobs, &chunk_keep)
            .await;
        let (chunked_outcomes, chunked_bytes) =
            self.pull_chunked(&chunked, out_root, overwrite, jobs).await;
        outcomes.extend(chunked_outcomes);
        (outcomes, plain_bytes.add(chunked_bytes))
    }

    /// The whole-file path: fetch every distinct blob once (grouped by hash) and
    /// materialize it at each destination path (#1306). A group whose hash is in
    /// `keep` (also a chunk of some chunked entry) leaves its staging blob in place
    /// for the chunked phase to reuse.
    async fn pull_plain(
        &self,
        entries: &[&ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        jobs: usize,
        keep: &HashSet<[u8; 32]>,
    ) -> (Vec<EntryOutcome>, Transfer) {
        let groups: Vec<Vec<EntryOutcome>> = futures_util::stream::iter(group_by_hash(entries))
            .map(|group| self.fetch_group(group, out_root, overwrite, keep))
            .buffer_unordered(jobs)
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

    /// The concatenation path for chunked entries, one unit of work per *file*
    /// (bounded by `jobs`) so at most `jobs` per-file bars are live at once:
    ///
    /// 1. **Fetch** each of a file's chunks — the first file to need a distinct
    ///    chunk fetches and **pays for it once** through the shared `claims`
    ///    ledger; a concurrent file that shares the chunk awaits that one result
    ///    and reuses the staged blob (never re-paying). Chunk blobs land in the
    ///    same content-addressed staging dir the whole-file path uses.
    /// 2. **Assemble** the file by concatenating its chunk staging files in order
    ///    and verifying the whole-file BLAKE3 ([`assemble_chunks`]), as soon as
    ///    its own chunks are ready.
    ///
    /// A chunk's staging blob is removed only when every entry that referenced it
    /// succeeded; otherwise it is kept as the resume prefix for a rerun (the same
    /// rule the whole-file path applies per group).
    async fn pull_chunked(
        &self,
        entries: &[&ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        jobs: usize,
    ) -> (Vec<EntryOutcome>, Transfer) {
        if entries.is_empty() {
            return (Vec::new(), Transfer::default());
        }

        // Resolve each entry to a plan before any fetch: a parse/path failure or
        // an already-present destination decides an outcome with no network cost.
        let plans: Vec<ChunkedPlan<'_>> = entries
            .iter()
            .map(|e| plan_chunked(e, out_root, overwrite))
            .collect();

        // The union of every assembled entry's chunks, for the final cleanup sweep.
        let mut all_chunks: HashSet<[u8; 32]> = HashSet::new();
        for plan in &plans {
            if let ChunkedPlan::Assemble { chunks, .. } = plan {
                all_chunks.extend(chunks.iter().copied());
            }
        }

        // The shared fetch-once ledger: the first file to need a chunk fetches (and
        // pays for) it and publishes the result into that chunk's cell; concurrent
        // files sharing it await the same cell and reuse the staged blob. A cell
        // caches its `Result` whether the fetch succeeded or failed, so a failed
        // shared chunk fails every dependent file without a re-fetch.
        let claims: tokio::sync::Mutex<HashMap<[u8; 32], ChunkCell>> =
            tokio::sync::Mutex::new(HashMap::new());

        // One future per FILE, bounded by `jobs`; results carry their plan index so
        // the outcome vector is restored to manifest order after the unordered run.
        let mut indexed: Vec<(usize, EntryOutcome)> =
            futures_util::stream::iter(plans.iter().enumerate())
                .map(|(idx, plan)| {
                    let claims = &claims;
                    async move {
                        let outcome = self.pull_chunked_file(plan, out_root, claims).await;
                        (idx, outcome)
                    }
                })
                .buffer_unordered(jobs)
                .collect::<Vec<_>>()
                .await;
        indexed.sort_by_key(|(idx, _)| *idx);
        let outcomes: Vec<EntryOutcome> = indexed.into_iter().map(|(_, o)| o).collect();

        // Read the resolved chunk results back out of the ledger for the tally and
        // the cleanup sweep. Every fetched chunk's cell is set by now (its file
        // future completed before the collect above returned).
        let fetched: HashMap<[u8; 32], Result<u64, String>> = {
            let guard = claims.lock().await;
            guard
                .iter()
                .filter_map(|(h, cell)| cell.get().map(|r| (*h, r.clone())))
                .collect()
        };

        // Cleanup: a chunk blob is safe to remove only if every entry that
        // referenced it produced a non-failed outcome. Otherwise keep it as the
        // resume prefix (its dependent entry, or its own fetch, failed).
        let mut chunk_failed: HashSet<[u8; 32]> = HashSet::new();
        for (plan, outcome) in plans.iter().zip(outcomes.iter()) {
            if let ChunkedPlan::Assemble { chunks, .. } = plan
                && matches!(outcome, EntryOutcome::Failed { .. })
            {
                chunk_failed.extend(chunks.iter().copied());
            }
        }
        for hash in &all_chunks {
            if !chunk_failed.contains(hash)
                && let Ok(staging) = staging_path(out_root, *hash)
            {
                remove_staging(&staging);
            }
        }

        // `downloaded` is the distinct chunk content bytes fetched (each shared
        // chunk once); `reconstructed` is the assembled file bytes written to
        // disk (a shared chunk counted in every file it composes).
        let downloaded = fetched
            .values()
            .filter_map(|r| r.as_ref().ok().copied())
            .fold(0u64, u64::saturating_add);
        let reconstructed = outcomes
            .iter()
            .filter_map(|o| match o {
                EntryOutcome::Fetched(n) => Some(*n),
                _ => None,
            })
            .fold(0u64, u64::saturating_add);

        (
            outcomes,
            Transfer {
                downloaded,
                reconstructed,
            },
        )
    }

    /// Fetch (or reuse) every chunk of one chunked entry, then assemble it. Each
    /// distinct chunk is fetched and paid for once through the shared `claims`
    /// ledger; a chunk another file already fetched is reused from staging. The
    /// per-file bar sums the file's chunks — advancing in real time for chunks this
    /// file fetches, and jumping by a whole chunk for ones it reuses.
    async fn pull_chunked_file(
        &self,
        plan: &ChunkedPlan<'_>,
        out_root: &Path,
        claims: &tokio::sync::Mutex<HashMap<[u8; 32], ChunkCell>>,
    ) -> EntryOutcome {
        let (label, chunks, size) = match plan {
            ChunkedPlan::Failed(o) => return o.clone(),
            // Already present — no fetch, no bar.
            ChunkedPlan::Skip => return EntryOutcome::Skipped,
            // The single destination path labels the file's bar.
            ChunkedPlan::Assemble {
                label,
                chunks,
                size,
                ..
            } => ((*label).to_string(), chunks, *size),
        };
        let cf = self.progress.chunked_file(label, size);

        // Resolve every chunk (fetch-once or reuse), collecting the per-chunk
        // results this file needs for assembly.
        let mut file_fetched: HashMap<[u8; 32], Result<u64, String>> = HashMap::new();
        for &chunk_hash in chunks {
            let cell = {
                let mut guard = claims.lock().await;
                Arc::clone(
                    guard
                        .entry(chunk_hash)
                        .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new())),
                )
            };
            // A private flag the fetch closure flips — only the file that actually
            // runs the fetch sets it, so a reusing file knows to jump its bar.
            let i_fetched = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let ran = Arc::clone(&i_fetched);
            let cb = cf.chunk_callback();
            let result = cell
                .get_or_init(|| async move {
                    ran.store(true, std::sync::atomic::Ordering::Relaxed);
                    self.fetch_chunk_staged(chunk_hash, out_root, cb.as_deref())
                        .await
                })
                .await;
            if !i_fetched.load(std::sync::atomic::Ordering::Relaxed)
                && let Ok(size) = result
            {
                // Reused a chunk another file fetched: its callback never fired
                // here, so advance this file's bar by the whole chunk at once.
                cf.advance_reused(*size);
            }
            file_fetched.insert(chunk_hash, result.clone());
        }

        let outcome = assemble_plan(plan, out_root, &file_fetched);
        cf.finish();
        outcome
    }

    /// Fetch one chunk blob into its content-addressed staging file and return its
    /// content size, or a formatted error. Mirrors the whole-file staging fetch,
    /// so a chunk gets the same reactive top-up and resume; `progress` drives the
    /// owning file's bar.
    async fn fetch_chunk_staged(
        &self,
        hash: [u8; 32],
        out_root: &Path,
        progress: Option<&ProgressCallback>,
    ) -> Result<u64, String> {
        match staging_path(out_root, hash) {
            Ok(staging) => match self.fetch_to_staging(hash, &staging, progress).await {
                Ok(()) => std::fs::metadata(&staging)
                    .map(|m| m.len())
                    .map_err(|e| format!("stat staged chunk: {e}")),
                Err(e) => Err(format!("{e:#}")),
            },
            Err(e) => Err(format!("{e:#}")),
        }
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
        keep: &HashSet<[u8; 32]>,
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

        let slots = plan_slots(&group.entries, out_root, overwrite);

        // Every destination already present (or failed to resolve) → no fetch, no
        // payment. This is the whole point of the group: a duplicate path that is
        // already on disk costs nothing.
        if !slots.iter().any(|s| matches!(s, Slot::Write { .. })) {
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
            .fetch_to_staging(hash, &staging, file_bar.callback())
            .await;
        file_bar.finish();
        if let Err(e) = fetched {
            // `drive_fetch` leaves its `<hex>.partial` + `.obao4`/`.ranges`
            // sidecars in place on error — they are what the next run resumes from
            // rather than re-paying for bytes already landed (same contract as
            // `fetch`'s `<output>.partial` store).
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
        // `drive_fetch` already finalized `<hex>` — a rerun sees the finalized
        // staging file and re-pulls only the still-missing ranges (typically
        // none), never the whole blob. Deleting staging here would force a full
        // re-fetch — and re-payment — of an unrefunded blob in *every* case;
        // `fetch`'s single-blob path gets this free from its own ranged store, so
        // the copy-based fan-out must gate it.
        //
        // A hash in `keep` is also a chunk of some chunked entry: leave it for the
        // chunked phase to reuse (paid once), which then removes it in its own
        // cleanup once every entry that needs it has landed.
        let any_failed = outcomes
            .iter()
            .any(|o| matches!(o, EntryOutcome::Failed { .. }));
        if should_remove_staging(any_failed, &hash, keep) {
            remove_staging(&staging);
        }

        outcomes
    }
}

/// Whether a whole-file group's staging blob may be removed after materializing:
/// only when nothing failed (a failed entry keeps its resume prefix) and the hash
/// is not also a chunk the chunked phase still needs (`keep`).
fn should_remove_staging(any_failed: bool, hash: &[u8; 32], keep: &HashSet<[u8; 32]>) -> bool {
    !any_failed && !keep.contains(hash)
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

/// A chunked entry resolved to what to do before any fetch: a failure fixed at
/// plan time (bad hash or unsafe path), an already-present destination to skip,
/// or an assembly with the whole-file hash, the ordered chunk hashes, and the
/// destination path.
enum ChunkedPlan<'a> {
    /// Resolve/parse failure — no chunk of this entry is fetched.
    Failed(EntryOutcome),
    /// Destination already present and `--overwrite` not set.
    Skip,
    /// Fetch these chunks and concatenate them into `dest`, verifying `whole`.
    Assemble {
        /// The entry's manifest path, retained to tag an outcome.
        label: &'a str,
        /// The whole-file BLAKE3 the assembled bytes must match.
        whole: [u8; 32],
        /// The chunk blob hashes, in content (concatenation) order.
        chunks: Vec<[u8; 32]>,
        /// The resolved on-disk destination.
        dest: PathBuf,
        /// The file's declared content size, for the total progress bar. `None`
        /// when the manifest entry omits it.
        size: Option<u64>,
    },
}

/// Resolve one chunked entry to a [`ChunkedPlan`] with no network activity: parse
/// its whole-file and chunk hashes, resolve+validate its destination path, and
/// apply skip-existing (a present final file is verified-good, so re-runs
/// resume). Mirrors [`plan_slots`] for the whole-file path.
fn plan_chunked<'a>(entry: &'a ManifestEntry, out_root: &Path, overwrite: bool) -> ChunkedPlan<'a> {
    let whole = match fetch::parse_hash(&entry.hash) {
        Ok(h) => h,
        Err(e) => return ChunkedPlan::Failed(EntryOutcome::failed(&entry.path, &e)),
    };
    let chunks = match parse_chunk_hashes(entry) {
        Ok(c) => c,
        Err(e) => return ChunkedPlan::Failed(EntryOutcome::failed(&entry.path, &e)),
    };
    let dest = match safe_join(out_root, &entry.path) {
        Ok(d) => d,
        Err(e) => return ChunkedPlan::Failed(EntryOutcome::failed(&entry.path, &e)),
    };
    // Reserve the staging dir, same as `plan_slots`: a destination inside it
    // would collide with a per-hash staging file and could be deleted by cleanup.
    if dest.starts_with(out_root.join(STAGING_DIR)) {
        return ChunkedPlan::Failed(EntryOutcome::failed(
            &entry.path,
            &anyhow!(
                "manifest path {:?} is inside the reserved staging directory {STAGING_DIR}/",
                entry.path
            ),
        ));
    }
    if !overwrite && dest.try_exists().unwrap_or(false) {
        return ChunkedPlan::Skip;
    }
    ChunkedPlan::Assemble {
        label: entry.path.as_str(),
        whole,
        chunks,
        dest,
        size: entry.size,
    }
}

/// Turn one resolved [`ChunkedPlan`] into an outcome: propagate a plan-time
/// failure or skip, else confirm every chunk fetched and assemble the file. A
/// chunk whose fetch failed (or is somehow absent) fails just this entry.
fn assemble_plan(
    plan: &ChunkedPlan<'_>,
    out_root: &Path,
    fetched: &HashMap<[u8; 32], Result<u64, String>>,
) -> EntryOutcome {
    let (label, whole, chunks, dest) = match plan {
        ChunkedPlan::Failed(o) => return o.clone(),
        ChunkedPlan::Skip => return EntryOutcome::Skipped,
        ChunkedPlan::Assemble {
            label,
            whole,
            chunks,
            dest,
            ..
        } => (label, whole, chunks, dest),
    };

    for h in chunks {
        match fetched.get(h) {
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                return EntryOutcome::Failed {
                    path: (*label).to_string(),
                    err: format!("chunk {}: {e}", blake3::Hash::from_bytes(*h).to_hex()),
                };
            }
            None => {
                return EntryOutcome::Failed {
                    path: (*label).to_string(),
                    err: format!(
                        "chunk {} was not fetched",
                        blake3::Hash::from_bytes(*h).to_hex()
                    ),
                };
            }
        }
    }

    let staging: Vec<PathBuf> = match chunks
        .iter()
        .map(|h| staging_path(out_root, *h))
        .collect::<anyhow::Result<Vec<_>>>()
    {
        Ok(v) => v,
        Err(e) => return EntryOutcome::failed(label, &e),
    };
    match assemble_chunks(&staging, dest, *whole) {
        Ok(n) => EntryOutcome::Fetched(n),
        Err(e) => EntryOutcome::failed(label, &e),
    }
}

/// Parse a chunked entry's chunk `hash` list into raw BLAKE3 addresses, in
/// content order. A single unparseable chunk hash fails the whole entry — the
/// concatenation is only meaningful if every piece resolves.
fn parse_chunk_hashes(entry: &ManifestEntry) -> anyhow::Result<Vec<[u8; 32]>> {
    entry
        .chunks
        .iter()
        .flatten()
        .map(|c| fetch::parse_hash(&c.hash))
        .collect()
}

/// Assemble a chunked entry's file at `dest` by concatenating the already-fetched
/// chunk blobs — `chunk_staging` in content order — and verifying the whole-file
/// BLAKE3 of the concatenation against `expected`. The bytes are streamed through
/// a hasher into a temp file beside `dest`; `dest` is created by an atomic rename
/// **only after** the hash matches, so a consumer never sees a half-assembled or
/// unverified file (the same "a present final file is verified-good" invariant
/// the plain path relies on). Each chunk blob was already BLAKE3-checked against
/// its own hash by the fetch path; this whole-file check additionally catches a
/// manifest whose chunk list is individually valid but wrong or misordered.
fn assemble_chunks(
    chunk_staging: &[PathBuf],
    dest: &Path,
    expected: [u8; 32],
) -> anyhow::Result<u64> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut tmp =
        fetch::temp_in_parent(dest).with_context(|| format!("stage {}", dest.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut total: u64 = 0;
    let mut buf = vec![0u8; 1 << 20];
    for chunk in chunk_staging {
        let mut src = std::fs::File::open(chunk)
            .with_context(|| format!("open chunk {}", chunk.display()))?;
        loop {
            let n = std::io::Read::read(&mut src, &mut buf)
                .with_context(|| format!("read chunk {}", chunk.display()))?;
            if n == 0 {
                break;
            }
            let slice = buf
                .get(..n)
                .ok_or_else(|| anyhow!("short read buffer slice"))?;
            hasher.update(slice);
            std::io::Write::write_all(tmp.as_file_mut(), slice)
                .with_context(|| format!("write {}", dest.display()))?;
            let n = u64::try_from(n).map_err(|_| anyhow!("chunk read size overflow"))?;
            total = total.saturating_add(n);
        }
    }
    let got = *hasher.finalize().as_bytes();
    if got != expected {
        // Drop the temp (never persisted) so `dest` stays absent — the assembled
        // bytes did not reconstruct the file the entry names.
        bail!(
            "assembled chunks hash {} does not match entry whole-file hash {}",
            blake3::Hash::from(got).to_hex(),
            blake3::Hash::from(expected).to_hex()
        );
    }
    tmp.as_file()
        .sync_all()
        .with_context(|| format!("sync staged assembly for {}", dest.display()))?;
    tmp.persist(dest)
        .map_err(|e| e.error)
        .with_context(|| format!("write {}", dest.display()))?;
    Ok(total)
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

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    #[tokio::test]
    async fn lane_lock_set_dedups_and_covers_every_provider() {
        let locks = LaneLocks::default();
        let (p1, p2, p3) = (addr(1), addr(2), addr(3));
        let guards = locks.lock_set(&[p2, p1, p2]).await;
        assert_eq!(guards.len(), 2, "duplicate providers lock once");
        // Every named provider's lane is held; an un-named one is free.
        assert!(locks.lock(p1).await.try_lock().is_err());
        assert!(locks.lock(p2).await.try_lock().is_err());
        assert!(locks.lock(p3).await.try_lock().is_ok());
        drop(guards);
        assert!(locks.lock(p1).await.try_lock().is_ok());
        assert!(locks.lock(p2).await.try_lock().is_ok());
    }

    #[tokio::test]
    async fn lane_lock_set_serializes_same_provider_across_entries() {
        let locks = LaneLocks::default();
        let p1 = addr(1);
        let guard = locks.lock(p1).await.lock_owned().await;
        // A second entry's lock-set touching the same lane cannot complete
        // while the first holds it, and completes once it is released.
        let providers = [p1];
        let pending = locks.lock_set(&providers);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "lock-set must wait on the held same-lane guard"
        );
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut pending)
            .await
            .expect("lock-set completes once the lane is released");
    }

    #[tokio::test]
    async fn lane_lock_set_overlapping_sets_do_not_deadlock() {
        // Two concurrent multi-source entries with overlapping provider sets
        // ({P1,P2} vs {P2,P3}): ordered acquisition means the second waits on
        // P2 without holding anything the first still needs, so both complete.
        let locks = LaneLocks::default();
        let (p1, p2, p3) = (addr(1), addr(2), addr(3));
        let held = tokio::sync::Notify::new();
        let first_dropped = std::cell::Cell::new(false);

        let first = async {
            let guards = locks.lock_set(&[p2, p1]).await;
            held.notify_one();
            // Hold both lanes across "streaming" so the second entry genuinely
            // blocks on P2 mid-acquisition.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(guards);
            first_dropped.set(true);
        };
        let second = async {
            held.notified().await;
            let _guards = locks.lock_set(&[p3, p2]).await;
            // P2 was held by the first entry when this acquisition started, so
            // completing proves it waited for the release rather than skipping.
            assert!(
                first_dropped.get(),
                "overlapping lane was acquired before its holder released it"
            );
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            futures_util::future::join(first, second).await;
        })
        .await
        .expect("overlapping lock-sets must not deadlock");
    }

    #[tokio::test]
    async fn lane_lock_set_opposite_orders_do_not_deadlock() {
        // Classic hold-and-wait cycle: {P1,P2} vs {P2,P1} deadlocks with naive
        // unordered acquisition; sorted global order (ADR 039) prevents it.
        // Both entries sort to [P1,P2], so the second waits on P1 rather than
        // holding P2 and waiting on P1.
        let locks = LaneLocks::default();
        let (p1, p2) = (addr(1), addr(2));
        let held = tokio::sync::Notify::new();
        let first_dropped = std::cell::Cell::new(false);

        let first = async {
            let guards = locks.lock_set(&[p1, p2]).await;
            held.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(guards);
            first_dropped.set(true);
        };
        let second = async {
            held.notified().await;
            let _guards = locks.lock_set(&[p2, p1]).await;
            assert!(
                first_dropped.get(),
                "opposite-order lane was acquired before holder released it"
            );
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            futures_util::future::join(first, second).await;
        })
        .await
        .expect("opposite-order lock-sets must not deadlock");
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
        let err = report(&outcomes, Transfer::default(), Path::new("/out"), true).unwrap_err();
        assert!(format!("{err:#}").contains("1 entr"), "{err:#}");
    }

    #[test]
    fn report_ok_when_none_failed() {
        let outcomes = vec![EntryOutcome::Fetched(10), EntryOutcome::Skipped];
        assert!(report(&outcomes, Transfer::default(), Path::new("/out"), false).is_ok());
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

    #[test]
    fn should_remove_staging_keeps_a_blob_the_chunked_phase_needs() {
        let h = [0x11; 32];
        let keep = HashSet::from([h]);
        let empty = HashSet::new();
        // Kept when the same blob is also a chunk — the chunked phase reuses it so
        // it is fetched and paid for once across both phases.
        assert!(!should_remove_staging(false, &h, &keep));
        // Removed when it is not needed elsewhere and nothing failed.
        assert!(should_remove_staging(false, &h, &empty));
        // A failed entry always keeps its resume prefix.
        assert!(!should_remove_staging(true, &h, &empty));
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
    /// place — the same gitignore separator rule `bundle create --exclude` uses.
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
    fn parse_chunk_hashes_preserves_order() {
        let e = ManifestEntry {
            path: "m".into(),
            hash: "b3:whole".into(),
            size: None,
            chunks: Some(vec![
                ManifestChunk {
                    hash: format!("b3:{}", "a".repeat(64)),
                },
                ManifestChunk {
                    hash: format!("b3:{}", "b".repeat(64)),
                },
            ]),
        };
        let got = parse_chunk_hashes(&e).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], [0xaa; 32]);
        assert_eq!(got[1], [0xbb; 32]);
    }

    #[test]
    fn parse_chunk_hashes_rejects_a_bad_chunk_hash() {
        let e = ManifestEntry {
            path: "m".into(),
            hash: "b3:whole".into(),
            size: None,
            chunks: Some(vec![ManifestChunk {
                hash: "not-a-hash".into(),
            }]),
        };
        assert!(parse_chunk_hashes(&e).is_err());
    }

    // Build a chunked entry whose two chunks concatenate to `whole_bytes`, and
    // return (entry, chunk0_hash, chunk1_hash) with real BLAKE3 content addresses.
    fn chunked_entry(path: &str, c0: &[u8], c1: &[u8]) -> (ManifestEntry, [u8; 32], [u8; 32]) {
        let h0 = *blake3::hash(c0).as_bytes();
        let h1 = *blake3::hash(c1).as_bytes();
        let mut whole = Vec::new();
        whole.extend_from_slice(c0);
        whole.extend_from_slice(c1);
        let hw = blake3::hash(&whole);
        let entry = ManifestEntry {
            path: path.into(),
            hash: format!("b3:{}", hw.to_hex()),
            size: Some(u64::try_from(whole.len()).unwrap()),
            chunks: Some(vec![
                ManifestChunk {
                    hash: format!("b3:{}", blake3::Hash::from_bytes(h0).to_hex()),
                },
                ManifestChunk {
                    hash: format!("b3:{}", blake3::Hash::from_bytes(h1).to_hex()),
                },
            ]),
        };
        (entry, h0, h1)
    }

    #[test]
    fn plan_chunked_skips_existing_destination() {
        let dir = tempfile::TempDir::new().unwrap();
        let (entry, _, _) = chunked_entry("a/m.bin", b"hello ", b"world");
        std::fs::create_dir_all(dir.path().join("a")).unwrap();
        std::fs::write(dir.path().join("a/m.bin"), b"already here").unwrap();

        assert!(matches!(
            plan_chunked(&entry, dir.path(), false),
            ChunkedPlan::Skip
        ));
    }

    #[test]
    fn plan_chunked_rejects_bad_whole_file_hash() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut entry = chunked_entry("m.bin", b"a", b"b").0;
        entry.hash = "not-a-hash".into();

        assert!(matches!(
            plan_chunked(&entry, dir.path(), false),
            ChunkedPlan::Failed(_)
        ));
    }

    #[test]
    fn assemble_plan_assembles_from_fetched_chunks() {
        let dir = tempfile::TempDir::new().unwrap();
        let (entry, h0, h1) = chunked_entry("out/m.bin", b"hello ", b"world");
        // Stage the two chunk blobs where `assemble_plan` will look for them.
        std::fs::write(staging_path(dir.path(), h0).unwrap(), b"hello ").unwrap();
        std::fs::write(staging_path(dir.path(), h1).unwrap(), b"world").unwrap();
        let plan = plan_chunked(&entry, dir.path(), false);
        let fetched: HashMap<[u8; 32], Result<u64, String>> =
            HashMap::from([(h0, Ok(6)), (h1, Ok(5))]);

        let outcome = assemble_plan(&plan, dir.path(), &fetched);

        assert!(matches!(outcome, EntryOutcome::Fetched(11)));
        assert_eq!(
            std::fs::read(dir.path().join("out/m.bin")).unwrap(),
            b"hello world"
        );
    }

    #[test]
    fn assemble_plan_fails_entry_when_a_chunk_fetch_failed() {
        let dir = tempfile::TempDir::new().unwrap();
        let (entry, h0, h1) = chunked_entry("m.bin", b"hello ", b"world");
        // Only the first chunk landed; the second failed to fetch.
        std::fs::write(staging_path(dir.path(), h0).unwrap(), b"hello ").unwrap();
        let plan = plan_chunked(&entry, dir.path(), false);
        let fetched: HashMap<[u8; 32], Result<u64, String>> =
            HashMap::from([(h0, Ok(6)), (h1, Err("upstream gone".to_string()))]);

        let outcome = assemble_plan(&plan, dir.path(), &fetched);

        match outcome {
            EntryOutcome::Failed { err, .. } => assert!(err.contains("upstream gone"), "{err}"),
            _ => panic!("expected Failed, got a success"),
        }
        // No half-file left behind.
        assert!(!dir.path().join("m.bin").exists());
    }

    #[test]
    fn assemble_chunks_concatenates_in_order_and_verifies() {
        let dir = tempfile::TempDir::new().unwrap();
        let c0 = dir.path().join("c0");
        std::fs::write(&c0, b"hello ").unwrap();
        let c1 = dir.path().join("c1");
        std::fs::write(&c1, b"world").unwrap();
        let dest = dir.path().join("out/file.bin");
        let expected = *blake3::hash(b"hello world").as_bytes();

        let n = assemble_chunks(&[c0, c1], &dest, expected).unwrap();

        assert_eq!(n, 11);
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello world");
    }

    #[test]
    fn assemble_chunks_rejects_whole_file_hash_mismatch() {
        let dir = tempfile::TempDir::new().unwrap();
        let c0 = dir.path().join("c0");
        std::fs::write(&c0, b"hello ").unwrap();
        let c1 = dir.path().join("c1");
        std::fs::write(&c1, b"world").unwrap();
        let dest = dir.path().join("file.bin");
        let wrong = *blake3::hash(b"not the concatenation").as_bytes();

        let err = assemble_chunks(&[c0, c1], &dest, wrong).unwrap_err();

        assert!(format!("{err:#}").contains("hash"), "{err:#}");
        // Nothing half-assembled is left behind for a caller to trust.
        assert!(!dest.exists(), "dest must be absent on mismatch");
    }

    #[test]
    fn assemble_chunks_order_is_load_bearing() {
        let dir = tempfile::TempDir::new().unwrap();
        let c0 = dir.path().join("c0");
        std::fs::write(&c0, b"hello ").unwrap();
        let c1 = dir.path().join("c1");
        std::fs::write(&c1, b"world").unwrap();
        let dest = dir.path().join("file.bin");
        // Hash of the in-order concatenation; passing the chunks reversed must fail.
        let expected = *blake3::hash(b"hello world").as_bytes();

        let err = assemble_chunks(&[c1, c0], &dest, expected).unwrap_err();

        assert!(format!("{err:#}").contains("hash"), "{err:#}");
    }

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
        assert!(report(&outcomes, Transfer::default(), Path::new("/out"), false).is_ok());
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
}
