//! `decdn bundle pull` — fetch every blob a bundle manifest references into an
//! output directory over the paid `cdn/client/v1` path (issue #391).
//!
//! The manifest comes from a local file (`-i`) or is fetched first by its own
//! BLAKE3 hash (`--hash`); either way entries are then fetched per *distinct*
//! blob hash. Entries naming the same blob (one file at two paths) are fetched —
//! and paid for — once and hard-linked (or copied) to each path (#1306). Node selection is
//! per blob (#936): with an explicit `--node-id` every entry is pulled from that
//! one node, otherwise each distinct blob discovers its own holder among the
//! region-nearest active nodes. A `DECDNMAN` entry reuses that holder for its
//! chunks, discovering again only after a typed delivery refusal. Distinct blobs
//! are fetched with `--jobs` concurrency.
//!
//! **Voucher-nonce safety.** A payment channel's vouchers use a strictly
//! increasing nonce, so two in-flight fetches sharing one channel would race it.
//! Concurrency is therefore bounded two ways: `--jobs` caps total in-flight
//! entries, and a per-provider async mutex serializes fetches that land on the
//! same provider's channel (which also makes the lazy open-or-reuse first-touch
//! race-free). Distinct providers proceed in parallel.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
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
use decdn_incentive::buyer_channel::BuyerChannelStore as _;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, PasswordSource, load_signer, read_password};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use futures_util::StreamExt as _;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayUrl};
use serde::{Deserialize, Serialize};

use super::chain_ctx;
use super::fetch;
use super::file_manifest;
use decdn_client_pull::discovery::{self, NodeCandidate};
use decdn_client_pull::endpoint as client_endpoint;
use decdn_client_pull::provider;
use decdn_client_pull::{PullDeadlines, UpstreamRefused};

type FetchTarget = (PublicKey, Address);

/// How the initial entry target was selected. Keeping this provenance beside
/// the target prevents reconstruction from accidentally enabling discovery for
/// an explicit `--node-id` pull.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectedTarget<T> {
    Pinned(T),
    Discovered(T),
}

impl<T: Copy> SelectedTarget<T> {
    const fn target(self) -> T {
        match self {
            Self::Pinned(target) | Self::Discovered(target) => target,
        }
    }

    const fn is_pinned(self) -> bool {
        matches!(self, Self::Pinned(_))
    }
}

/// Remove the node that just refused delivery before probing fallback holders.
/// The node id is the delivery endpoint identity; excluding it also protects
/// against a stale duplicate registry row carrying a different provider address.
fn fallback_candidates(candidates: &[NodeCandidate], refusing: FetchTarget) -> Vec<NodeCandidate> {
    candidates
        .iter()
        .filter(|candidate| candidate.node_id != refusing.0)
        .cloned()
        .collect()
}

/// Group manifest entries by their blob `hash`, preserving first-seen order both
/// across groups and within each group. Entries that name the same blob (one
/// file published at two paths) land in one group so it is fetched once (#1306).
fn group_by_hash(entries: &[ManifestEntry]) -> Vec<HashGroup<'_>> {
    let mut index: HashMap<&str, usize> = HashMap::new();
    let mut groups: Vec<HashGroup> = Vec::new();
    for entry in entries {
        let next = groups.len();
        let at = *index.entry(entry.hash.as_str()).or_insert(next);
        if at == next {
            groups.push(HashGroup {
                hash: entry.hash.as_str(),
                entries: vec![entry],
            });
        } else if let Some(group) = groups.get_mut(at) {
            group.entries.push(entry);
        }
    }
    groups
}

/// Materialize `src`'s content at `dest` without re-reading it over the network:
/// a hard link where the filesystem allows it, else a full copy (cross-device
/// `EXDEV`, or a filesystem that can't link). Staged in `dest`'s parent and
/// renamed into place so `dest` is only ever absent or complete — the same
/// atomic-replace invariant `write_blob_atomic` upholds, which bundle pull's
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
/// paid path (for a `DECDNMAN` entry it fetches the chunks) and runs **at most
/// once** per group — every later destination goes through the free `link`,
/// reported as [`EntryOutcome::Linked`] so the summary never implies a second
/// paid fetch. If the first materialize fails, the next writable path retries it
/// from the same in-memory bytes (no re-fetch), so one bad path can't doom the
/// group.
///
/// Parameterized over the two operations so the fetch-once / link-rest invariant
/// — the core of #1306 — is unit-testable without a live endpoint or channel.
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

/// Fetch from `preferred`; on an auto-discovered delivery refusal only, discover
/// a holder for this hash, retry there, and carry a successful fallback forward.
async fn fetch_preferred<T, Fetch, FetchFut, Discover, DiscoverFut>(
    hash: [u8; 32],
    preferred: &Cell<SelectedTarget<T>>,
    fetch_from: Fetch,
    discover: Discover,
) -> anyhow::Result<Vec<u8>>
where
    T: Copy,
    Fetch: Fn([u8; 32], T) -> FetchFut,
    FetchFut: std::future::Future<Output = anyhow::Result<Vec<u8>>>,
    Discover: FnOnce([u8; 32], T) -> DiscoverFut,
    DiscoverFut: std::future::Future<Output = anyhow::Result<T>>,
{
    let selected = preferred.get();
    let target = selected.target();
    match fetch_from(hash, target).await {
        Ok(bytes) => Ok(bytes),
        Err(err)
            if !selected.is_pinned()
                && err
                    .downcast_ref::<UpstreamRefused>()
                    .is_some_and(|refused| refused.error.is_delivery_side()) =>
        {
            let fallback = discover(hash, target).await?;
            let bytes = fetch_from(hash, fallback).await?;
            preferred.set(SelectedTarget::Discovered(fallback));
            Ok(bytes)
        }
        Err(err) => Err(err),
    }
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
enum EntryOutcome {
    Fetched(u64),
    Linked,
    Skipped,
    Failed { path: String, err: String },
}

/// One-line `--json` summary. `fetched`/`total_bytes` count only paid network
/// pulls; `linked` counts duplicate destinations satisfied locally.
#[derive(Serialize)]
struct PullReport {
    output: String,
    fetched: u64,
    linked: u64,
    skipped: u64,
    failed: u64,
    total_bytes: u64,
}

/// Read the registry once and keep the region-nearest candidates, which every
/// entry in the manifest then reuses.
async fn discover_candidates(
    chain: &fetch::ResolvedChain,
) -> anyhow::Result<Vec<discovery::NodeCandidate>> {
    let capacity_bond = chain.capacity_bond.ok_or_else(|| {
        anyhow!(
            "auto-discovery needs capacity_bond_address (--capacity-bond-address or \
             blockchain.capacity_bond_address), or pass --node-id to pull from one node"
        )
    })?;
    let bootstrap =
        discovery::bootstrap_nodes(&chain.rpc_url, capacity_bond, &chain.data_dir).await?;
    // See `fetch::discover_provider`: the degraded bootstrap paths reach the
    // user through the return value, because the CLI has no log sink.
    if let Some(warning) = bootstrap.warning() {
        eprintln!("{warning}");
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

/// Entry point for `decdn bundle pull`.
pub async fn bundle_pull(args: &BundlePullArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    // Dry-run short-circuits before any network/chain/keystore activity.
    if args.dry_run {
        return dry_run(args);
    }
    // As in `fetch`: reject a hard cap that would silently disable stall detection.
    args.common.validate()?;

    // A local manifest is read up front (no network): an empty bundle then needs
    // no endpoint or keystore password at all.
    let local_manifest = match &args.input {
        Some(path) => Some(read_local_manifest(path)?),
        None => None,
    };
    if let Some(m) = &local_manifest
        && m.entries.is_empty()
    {
        println!("bundle has no entries; nothing to fetch");
        return Ok(());
    }

    let common = &args.common;
    let relays = client_endpoint::resolve_relays(common.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let file = load_file_config(config_path)?;
    let chain = fetch::resolve_chain(common, &file)?;
    let store = RedbBuyerChannelStore::open(&chain.data_dir)?;
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;

    // Selection: explicit single node for every entry, or a per-entry discovery
    // candidate list read from `CapacityBond` once.
    let explicit = explicit_target(common)?;
    let candidates = match explicit {
        Some(_) => None,
        None => Some(discover_candidates(&chain).await?),
    };

    // Buyer signer (vouchers + any openChannel tx). Prompted only once, after we
    // know there is work to do. Password from env, else TTY.
    let password = read_password(
        &[
            PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV),
            PasswordSource::Prompt { confirm: false },
        ],
        "eth keystore password",
    )?;
    let signer = Arc::new(load_signer(&chain.keystore, &password)?);
    let self_address = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentChannel::new(chain.payment_channel, rpc.clone());
    let voucher_dom = voucher_domain(chain.chain_id, chain.payment_channel);
    let slash_dom = slash_judge_domain(chain.chain_id, chain.slash_judge);

    let ctx = PullCtx {
        endpoint: &endpoint,
        store: &store,
        contract: &contract,
        rpc: &rpc,
        signer: &signer,
        self_address,
        voucher_dom: &voucher_dom,
        slash_dom: &slash_dom,
        chain: &chain,
        relays: &relays,
        explicit,
        candidates,
        common,
        locks: RefCell::new(HashMap::new()),
        open_lock: tokio::sync::Mutex::new(()),
    };

    // Obtain the manifest: the pre-read local one, or fetch the bundle blob.
    let manifest = if let Some(m) = local_manifest {
        m
    } else {
        let raw = args
            .hash
            .as_deref()
            .ok_or_else(|| anyhow!("no bundle source (expected -i or --hash)"))?;
        let bytes = ctx
            .fetch(fetch::parse_hash(raw)?)
            .await
            .context("fetch bundle manifest blob")?;
        parse_manifest(&bytes)?
    };
    if manifest.entries.is_empty() {
        println!("bundle has no entries; nothing to fetch");
        return Ok(());
    }

    let outcomes = ctx
        .pull_all(
            &manifest.entries,
            &args.output,
            args.overwrite,
            args.jobs.max(1),
        )
        .await;

    report(&outcomes, &args.output, args.json)
}

/// Shared, by-reference state for the entry fetch loop. Borrowed by every
/// in-flight entry future; single-task `buffer_unordered` means interior
/// mutability (`RefCell`/`Rc`) is sufficient — no `Send`/`Sync` needed.
struct PullCtx<'a, P: Provider + Clone> {
    endpoint: &'a Endpoint,
    store: &'a RedbBuyerChannelStore,
    contract: &'a PaymentChannel::PaymentChannelInstance<P>,
    rpc: &'a P,
    signer: &'a Arc<PrivateKeySigner>,
    self_address: Address,
    voucher_dom: &'a Eip712Domain,
    slash_dom: &'a Eip712Domain,
    chain: &'a fetch::ResolvedChain,
    relays: &'a [RelayUrl],
    /// `Some((node_id, provider))` pins every entry to one node (`--node-id`);
    /// `None` discovers per entry against `candidates`.
    explicit: Option<FetchTarget>,
    candidates: Option<Vec<NodeCandidate>>,
    common: &'a ClientFetchArgs,
    /// Per-provider locks: serialize fetches sharing one channel's voucher
    /// nonce. Lazily created; held only across one entry's fetch.
    locks: RefCell<HashMap<Address, Rc<tokio::sync::Mutex<()>>>>,
    /// Serializes channel *opens* across all providers. The buyer's USDC
    /// allowance for the `PaymentChannel` is a single owner→spender slot, and the
    /// client default approves it to the exact per-open deposit (ERC-20 `approve`
    /// overwrites, not accumulates). Two concurrent opens against distinct
    /// providers would otherwise race that slot and the second `openChannel`'s
    /// `transferFrom` would revert. Taken only for an actual open (not a
    /// live-channel reuse, which issues no approval) and released before
    /// streaming, so reuse and blob delivery still run concurrently.
    open_lock: tokio::sync::Mutex<()>,
}

impl<P: Provider + Clone> PullCtx<'_, P> {
    /// The per-provider lock, created on first use.
    fn provider_lock(&self, provider: Address) -> Rc<tokio::sync::Mutex<()>> {
        let mut map = self.locks.borrow_mut();
        Rc::clone(
            map.entry(provider)
                .or_insert_with(|| Rc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Select the node to fetch `hash` from: the pinned explicit node, or
    /// per-entry discovery over the shared candidate list.
    async fn pick(&self, hash: [u8; 32]) -> anyhow::Result<SelectedTarget<FetchTarget>> {
        if let Some(pinned) = self.explicit {
            return Ok(SelectedTarget::Pinned(pinned));
        }
        Ok(SelectedTarget::Discovered(
            self.pick_excluding(hash, None).await?,
        ))
    }

    /// Select a target while omitting a node that already refused this hash.
    async fn pick_excluding(
        &self,
        hash: [u8; 32],
        excluded: Option<FetchTarget>,
    ) -> anyhow::Result<FetchTarget> {
        let candidates = self
            .candidates
            .as_deref()
            .ok_or_else(|| anyhow!("no discovery candidates available"))?;
        let filtered = excluded.map(|target| fallback_candidates(candidates, target));
        let candidates = filtered.as_deref().unwrap_or(candidates);
        let picked = fetch::probe_and_rank(
            self.endpoint,
            self.store,
            candidates,
            self.relays.first(),
            hash,
            fetch::ProxyWarmingParams::from_args(self.common),
        )
        .await?;
        Ok((picked.node_id, picked.eth_address))
    }

    /// Fetch one blob after normal explicit selection or discovery.
    async fn fetch(&self, hash: [u8; 32]) -> anyhow::Result<Vec<u8>> {
        Ok(self.fetch_with_target(hash).await?.0)
    }

    /// Fetch one blob and retain the target chosen for it.
    async fn fetch_with_target(
        &self,
        hash: [u8; 32],
    ) -> anyhow::Result<(Vec<u8>, SelectedTarget<FetchTarget>)> {
        let selected = self.pick(hash).await?;
        let bytes = self.fetch_from(hash, selected.target()).await?;
        Ok((bytes, selected))
    }

    /// Fetch directly from `node_id`/`provider`, bypassing discovery.
    async fn fetch_from(
        &self,
        hash: [u8; 32],
        (node_id, provider): FetchTarget,
    ) -> anyhow::Result<Vec<u8>> {
        // Serialize all access to this provider's channel: the open-or-reuse +
        // voucher-signing critical section must be atomic per channel.
        let lock = self.provider_lock(provider);
        let _guard = lock.lock().await;

        // Only an actual channel *open* touches the shared USDC allowance, so
        // only opens take the global `open_lock`. A live-channel reuse issues no
        // approval and — under the per-provider lock held above, which gives this
        // provider's channel state exclusive access — cannot turn into an open, so
        // it stays lock-free and concurrent with another provider's in-flight open
        // (which can take minutes on-chain).
        let reuse_only = self
            .store
            .get_by_provider(provider)?
            .is_some_and(|state| !state.is_expired_at(fetch::unix_now()));
        let ctx = {
            let _open_guard = if reuse_only {
                None
            } else {
                Some(self.open_lock.lock().await)
            };
            fetch::open_or_reuse(
                self.store,
                self.contract,
                self.rpc,
                self.signer,
                self.voucher_dom,
                provider,
                self.self_address,
                self.chain.payment_channel,
                self.chain.deposit,
                self.chain.max_approve,
            )
            .await?
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

        let max_blob_bytes = self.common.max_blob_mb.saturating_mul(1024 * 1024);
        fetch::fetch_blob(
            self.endpoint,
            target,
            &ctx,
            self.slash_dom,
            provider,
            self.store,
            hash,
            // Same shape as `fetch` (#1134): a node that accepts the connection and never
            // answers is as dead as one that stops mid-stream, so the same budget bounds
            // both stages, under a cap that must outlast them both.
            PullDeadlines::capped(
                self.common.stall_timeout(),
                self.common.stall_timeout(),
                self.common.hard_cap(),
            )?,
            max_blob_bytes,
            // Per-entry byte bars would interleave illegibly across a manifest's
            // many concurrent pulls; `bundle pull` reports at entry granularity
            // instead (#1118 scopes the byte bar to single-blob `fetch`).
            None,
        )
        .await
    }

    /// Fetch one `DECDNMAN` chunk from its preferred target, with discovery
    /// fallback enabled only for the auto-discovered bundle-pull path.
    async fn fetch_manifest_chunk(
        &self,
        hash: [u8; 32],
        preferred: &Cell<SelectedTarget<FetchTarget>>,
    ) -> anyhow::Result<Vec<u8>> {
        fetch_preferred(
            hash,
            preferred,
            |hash, target| self.fetch_from(hash, target),
            |hash, refusing| self.pick_excluding(hash, Some(refusing)),
        )
        .await
    }

    /// Fetch every entry into `out_root`, one unit of work per *distinct* blob
    /// hash: entries sharing a hash (one file at two bundle paths) are fetched and
    /// reconstructed once, then materialized at each path (#1306) — never fetched,
    /// nor *paid for*, twice. `buffer_unordered` polls up to `jobs` futures in this
    /// one task (no `tokio::spawn`: `probe_once` is not `Send`); parallelism comes
    /// from concurrent in-flight network I/O, while the per-provider locks inside
    /// `fetch` serialize same-channel access — now over unique blobs.
    async fn pull_all(
        &self,
        entries: &[ManifestEntry],
        out_root: &Path,
        overwrite: bool,
        jobs: usize,
    ) -> Vec<EntryOutcome> {
        futures_util::stream::iter(group_by_hash(entries))
            .map(|group| self.fetch_group(group, out_root, overwrite))
            .buffer_unordered(jobs)
            .collect::<Vec<Vec<EntryOutcome>>>()
            .await
            .into_iter()
            .flatten()
            .collect()
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

        let (bytes, target) = match self.fetch_with_target(hash).await {
            Ok(fetched) => fetched,
            Err(e) => {
                return slots
                    .into_iter()
                    .map(|s| match s {
                        Slot::Failed(o) => o,
                        Slot::Skip => EntryOutcome::Skipped,
                        Slot::Write { label, .. } => EntryOutcome::failed(label, &e),
                    })
                    .collect();
            }
        };

        // Borrow `bytes` (not move it) into the `FnMut`: `materialize` runs once,
        // but a retry after a failed first write may call it again.
        let bytes = &bytes;
        materialize_group(
            slots,
            |dest| self.materialize(bytes, hash, dest, target),
            link_or_copy_atomic,
        )
        .await
    }

    /// Write already-fetched `bytes` to `dest`, returning the byte count. A bundle
    /// entry's blob may itself be a `DECDNMAN` file manifest — the two layers
    /// compose (`appendix-bundles.md` § Non-relationship to ADR 012's `DECDNMAN`
    /// chunk manifest) — in which case the real bytes are its chunks (#1183),
    /// reconstructed here. Without the magic these ARE the bytes; write as-is.
    async fn materialize(
        &self,
        bytes: &[u8],
        hash: [u8; 32],
        dest: PathBuf,
        target: SelectedTarget<FetchTarget>,
    ) -> anyhow::Result<u64> {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        if let Some(manifest) = file_manifest::sniff(bytes) {
            return self.reconstruct_entry(manifest, hash, &dest, target).await;
        }
        fetch::write_blob_atomic(&dest, bytes)
            .with_context(|| format!("write {}", dest.display()))?;
        Ok(bytes.len() as u64)
    }

    /// Expand a manifest entry whose blob turned out to be a file manifest:
    /// fetch and verify its chunks in order, then reconstruct into `dest`.
    ///
    /// Chunk pulls first target the node that served the manifest. A typed
    /// delivery refusal triggers normal per-hash discovery, and a successful
    /// fallback becomes the preferred target for later chunks. Explicit
    /// `--node-id` pulls remain pinned. Per-provider channel serialization is
    /// still released between sequential chunks, while sibling entries retain
    /// their independent selection policy.
    async fn reconstruct_entry(
        &self,
        manifest: anyhow::Result<file_manifest::FileManifest>,
        manifest_hash: [u8; 32],
        dest: &Path,
        target: SelectedTarget<FetchTarget>,
    ) -> anyhow::Result<u64> {
        let manifest = manifest?;
        // A given manifest blob is reconstructed at most once per bundle pull:
        // entries are grouped by hash before the fan-out (#1306), so the two
        // paths of a file duplicated in the bundle share one reconstruction and
        // the second is a hard link, not a second paid fetch. Cross-process
        // serialization of the shared part directory (two `decdn` invocations on
        // one data dir) is handled by the `flock` inside `file_manifest::
        // reconstruct` (#1303); no process-local lock is needed here.
        let preferred = Cell::new(target);
        file_manifest::reconstruct(
            &manifest,
            manifest_hash,
            &file_manifest::downloads_root(&self.chain.data_dir),
            dest,
            !self.common.no_keep_blobs,
            |chunk_hash| self.fetch_manifest_chunk(chunk_hash, &preferred),
        )
        .await
    }
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

/// Print the would-fetch plan and exit (no network/chain/keystore activity).
/// With `--hash` the entries can't be enumerated offline, so only the intent is
/// reported.
fn dry_run(args: &BundlePullArgs) -> anyhow::Result<()> {
    let out = args.output.display();
    match (&args.input, &args.hash) {
        (Some(path), _) => {
            let manifest = read_local_manifest(path)?;
            if args.json {
                let plan = serde_json::json!({
                    "output": args.output.display().to_string(),
                    "count": manifest.entries.len(),
                    "entries": manifest.entries.iter().map(|e| serde_json::json!({
                        "path": e.path, "hash": e.hash, "size": e.size,
                    })).collect::<Vec<_>>(),
                });
                println!("{plan}");
            } else {
                println!(
                    "would fetch {} entr(ies) into {out}:",
                    manifest.entries.len()
                );
                for e in &manifest.entries {
                    match e.size {
                        Some(n) => println!("  {} ({n} bytes)", e.path),
                        None => println!("  {}", e.path),
                    }
                }
            }
        }
        (None, Some(h)) => {
            println!(
                "--dry-run with --hash: would fetch bundle {h} then its entries into {out} \
                 (entries are not enumerable without fetching the manifest)"
            );
        }
        (None, None) => bail!("no bundle source (expected -i or --hash)"),
    }
    Ok(())
}

/// Summarize outcomes; return an error if any entry failed (after reporting all).
fn report(outcomes: &[EntryOutcome], output: &Path, json: bool) -> anyhow::Result<()> {
    let mut fetched = 0u64;
    let mut linked = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;
    let mut total_bytes = 0u64;
    for o in outcomes {
        match o {
            // `total_bytes` counts only paid network pulls; a linked duplicate
            // adds a file on disk but no wire bytes and no payment (#1306).
            EntryOutcome::Fetched(n) => {
                fetched += 1;
                total_bytes = total_bytes.saturating_add(*n);
            }
            EntryOutcome::Linked => linked += 1,
            EntryOutcome::Skipped => skipped += 1,
            EntryOutcome::Failed { path, err } => {
                failed += 1;
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
        total_bytes,
    };
    if json {
        let line = serde_json::to_string(&rep).map_err(|e| anyhow!("serialize report: {e}"))?;
        println!("{line}");
    } else {
        println!(
            "pulled into {} ({fetched} fetched, {linked} linked, {skipped} skipped, \
             {failed} failed, {total_bytes} bytes)",
            output.display()
        );
    }

    if failed > 0 {
        bail!("{failed} entr(ies) failed to fetch");
    }
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use decdn_protocol::Region;

    use super::*;

    #[tokio::test]
    async fn manifest_chunks_reuse_the_preferred_target_without_discovery() {
        let preferred = std::cell::Cell::new(SelectedTarget::Discovered(1u8));
        let fetches = Rc::new(RefCell::new(Vec::new()));
        let discoveries = Rc::new(RefCell::new(0u8));

        for hash in [[1u8; 32], [2u8; 32]] {
            fetch_preferred(
                hash,
                &preferred,
                {
                    let fetches = Rc::clone(&fetches);
                    move |hash, target| {
                        let fetches = Rc::clone(&fetches);
                        async move {
                            fetches.borrow_mut().push((hash, target));
                            Ok(vec![hash[0]])
                        }
                    }
                },
                {
                    let discoveries = Rc::clone(&discoveries);
                    move |_, _| {
                        let discoveries = Rc::clone(&discoveries);
                        async move {
                            *discoveries.borrow_mut() += 1;
                            Ok(9u8)
                        }
                    }
                },
            )
            .await
            .unwrap();
        }

        assert_eq!(
            fetches.borrow().as_slice(),
            &[([1u8; 32], 1), ([2u8; 32], 1)]
        );
        assert_eq!(*discoveries.borrow(), 0);
        assert_eq!(preferred.get(), SelectedTarget::Discovered(1));
    }

    #[tokio::test]
    async fn delivery_refusal_discovers_once_and_carries_the_fallback_forward() {
        let preferred = std::cell::Cell::new(SelectedTarget::Discovered(1u8));
        let fetches = Rc::new(RefCell::new(Vec::new()));
        let discoveries = Rc::new(RefCell::new(0u8));

        for hash in [[1u8; 32], [2u8; 32]] {
            fetch_preferred(
                hash,
                &preferred,
                {
                    let fetches = Rc::clone(&fetches);
                    move |hash, target| {
                        let fetches = Rc::clone(&fetches);
                        async move {
                            fetches.borrow_mut().push((hash, target));
                            if hash == [1u8; 32] && target == 1 {
                                Err(anyhow::Error::new(decdn_client_pull::UpstreamRefused {
                                    error: decdn_protocol::StreamError::EvictedSinceProbe,
                                }))
                            } else {
                                Ok(vec![hash[0]])
                            }
                        }
                    }
                },
                {
                    let discoveries = Rc::clone(&discoveries);
                    move |_, _| {
                        let discoveries = Rc::clone(&discoveries);
                        async move {
                            *discoveries.borrow_mut() += 1;
                            Ok(2u8)
                        }
                    }
                },
            )
            .await
            .unwrap();
        }

        assert_eq!(
            fetches.borrow().as_slice(),
            &[([1u8; 32], 1), ([1u8; 32], 2), ([2u8; 32], 2),]
        );
        assert_eq!(*discoveries.borrow(), 1);
        assert_eq!(preferred.get(), SelectedTarget::Discovered(2));
    }

    #[tokio::test]
    async fn delivery_refusal_excludes_the_refusing_target_from_discovery() {
        let preferred = std::cell::Cell::new(SelectedTarget::Discovered(1u8));
        let excluded = Rc::new(RefCell::new(Vec::new()));

        let bytes = fetch_preferred(
            [1u8; 32],
            &preferred,
            |hash, target| async move {
                if target == 1 {
                    Err(anyhow::Error::new(decdn_client_pull::UpstreamRefused {
                        error: decdn_protocol::StreamError::Overloaded,
                    }))
                } else {
                    Ok(vec![hash[0]])
                }
            },
            {
                let excluded = Rc::clone(&excluded);
                move |_, refusing_target| {
                    let excluded = Rc::clone(&excluded);
                    async move {
                        excluded.borrow_mut().push(refusing_target);
                        Ok(2u8)
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(bytes, vec![1]);
        assert_eq!(excluded.borrow().as_slice(), &[1]);
        assert_eq!(preferred.get(), SelectedTarget::Discovered(2));
    }

    #[test]
    fn fallback_candidates_omit_the_refusing_node() {
        let refusing = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let alternative = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let refusing_provider = Address::repeat_byte(1);
        let candidates = vec![
            NodeCandidate {
                node_id: refusing,
                eth_address: refusing_provider,
                region_hint: Region::parse("TR"),
            },
            NodeCandidate {
                node_id: alternative,
                eth_address: Address::repeat_byte(2),
                region_hint: Region::parse("TR"),
            },
        ];

        let filtered = fallback_candidates(&candidates, (refusing, refusing_provider));

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].node_id, alternative);
    }

    #[tokio::test]
    async fn explicit_pin_does_not_discover_after_not_found() {
        let preferred = std::cell::Cell::new(SelectedTarget::Pinned(1u8));
        let discoveries = Rc::new(RefCell::new(0u8));

        let err = fetch_preferred(
            [1u8; 32],
            &preferred,
            |_, _| async {
                Err(anyhow::Error::new(decdn_client_pull::UpstreamRefused {
                    error: decdn_protocol::StreamError::NotFound,
                }))
            },
            {
                let discoveries = Rc::clone(&discoveries);
                move |_, _| {
                    let discoveries = Rc::clone(&discoveries);
                    async move {
                        *discoveries.borrow_mut() += 1;
                        Ok(2u8)
                    }
                }
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err.downcast_ref::<decdn_client_pull::UpstreamRefused>(),
                Some(refused)
                    if matches!(refused.error, decdn_protocol::StreamError::NotFound)
            ),
            "{err:#}"
        );
        assert_eq!(*discoveries.borrow(), 0);
        assert_eq!(preferred.get(), SelectedTarget::Pinned(1));
    }

    /// The delivery-side gate is load-bearing in the money sense, so pin it from
    /// this side too (`StreamError`'s own domain split is covered in
    /// `decdn_protocol`). `VoucherRejected` is the one code that arrives ONLY
    /// mid-stream — after bytes were delivered and paid for — and it reports a
    /// buyer-side voucher fault that would follow us to the next node. Falling
    /// back on it would re-fetch and re-pay the chunk to lose the same way twice,
    /// once per remaining chunk.
    #[tokio::test]
    async fn voucher_rejection_does_not_fall_back() {
        let preferred = std::cell::Cell::new(SelectedTarget::Discovered(1u8));
        let fetches = Rc::new(RefCell::new(0u8));
        let discoveries = Rc::new(RefCell::new(0u8));

        let err = fetch_preferred(
            [1u8; 32],
            &preferred,
            {
                let fetches = Rc::clone(&fetches);
                move |_, _| {
                    let fetches = Rc::clone(&fetches);
                    async move {
                        *fetches.borrow_mut() += 1;
                        Err(anyhow::Error::new(decdn_client_pull::UpstreamRefused {
                            error: decdn_protocol::StreamError::VoucherRejected {
                                reason: decdn_protocol::VoucherRejectReason::WrongSigner,
                            },
                        }))
                    }
                }
            },
            {
                let discoveries = Rc::clone(&discoveries);
                move |_, _| {
                    let discoveries = Rc::clone(&discoveries);
                    async move {
                        *discoveries.borrow_mut() += 1;
                        Ok(2u8)
                    }
                }
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err.downcast_ref::<decdn_client_pull::UpstreamRefused>(),
                Some(refused) if refused.error.is_mid_stream()
            ),
            "{err:#}"
        );
        assert_eq!(*fetches.borrow(), 1, "the refusal must not be retried");
        assert_eq!(*discoveries.borrow(), 0);
        assert_eq!(preferred.get(), SelectedTarget::Discovered(1));
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
    fn parse_manifest_accepts_v1_with_optional_size() {
        let json = br#"{"version":1,"entries":[{"path":"a.txt","hash":"b3:ab","size":4},{"path":"b","hash":"b3:cd"}]}"#;
        let m = parse_manifest(json).unwrap();
        assert_eq!(m.entries.len(), 2);
        assert_eq!(m.entries[0].size, Some(4));
        assert_eq!(m.entries[1].size, None);
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
        let err = report(&outcomes, Path::new("/out"), true).unwrap_err();
        assert!(format!("{err:#}").contains("1 entr"), "{err:#}");
    }

    #[test]
    fn report_ok_when_none_failed() {
        let outcomes = vec![EntryOutcome::Fetched(10), EntryOutcome::Skipped];
        assert!(report(&outcomes, Path::new("/out"), false).is_ok());
    }

    fn entry(path: &str, hash: &str) -> ManifestEntry {
        ManifestEntry {
            path: path.into(),
            hash: hash.into(),
            size: None,
        }
    }

    /// The core of #1306: entries naming the same blob collapse into one group, so
    /// the fan-out fetches (and pays for) that blob exactly once. First-seen order
    /// is preserved both across groups and within a group.
    #[test]
    fn group_by_hash_collapses_duplicates_preserving_order() {
        let entries = vec![
            entry("a.txt", "b3:h1"),
            entry("b.txt", "b3:h2"),
            entry("c.txt", "b3:h1"),
        ];
        let paths: Vec<Vec<&str>> = group_by_hash(&entries)
            .iter()
            .map(|group| group.entries.iter().map(|e| e.path.as_str()).collect())
            .collect();
        assert_eq!(paths, vec![vec!["a.txt", "c.txt"], vec!["b.txt"]]);
    }

    /// Distinct hashes never merge — each is its own unit of work, in order.
    #[test]
    fn group_by_hash_keeps_distinct_hashes_separate() {
        let entries = vec![entry("a", "b3:1"), entry("b", "b3:2"), entry("c", "b3:3")];
        let groups = group_by_hash(&entries);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.entries.len() == 1));
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
        assert!(report(&outcomes, Path::new("/out"), false).is_ok());
    }
}
