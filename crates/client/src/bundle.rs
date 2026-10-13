//! Pull a whole bundle: every blob a bundle manifest names, into an output
//! directory, over the paid `cdn/client/v1` path.
//!
//! A bundle is a JSON manifest of `(path, hash, size, chunks)` entries
//! (`adr/appendix-bundles.md`). [`BundlePuller`] fetches each distinct blob
//! once, from one shared payment pool, and lands it at every path that names
//! it. It resumes from what an earlier run left: a file already on disk is
//! skipped, a byte-identical file elsewhere in the output directory is linked,
//! a range a sibling entry shares is spliced from disk, and a `.partial` an
//! interrupted run left is the next fetch's prefix.
//!
//! # A pull, end to end
//!
//! 1. Resolve the holders once for the run with [`select_nodes`]: the pinned
//!    node, or every active registered node for each entry to sample.
//! 2. Build a [`BundlePuller`] over the chain, the local site, an endpoint, a
//!    buyer-pool store and the [`Funding`] the pull pays from.
//! 3. Get the manifest: [`BundleManifest::parse`] a local file, or
//!    [`BundlePuller::fetch_manifest`] it by its hash.
//! 4. Drop the entries you do not want, then [`BundlePuller::pull`] the rest.
//!    Progress reaches a [`BundleObserver`]; the result is a [`BundleOutcome`].
//! 5. [`BundlePuller::finish`] waits for the lanes' watermark writes.
//!
//! The pull future is not `Send`: every entry and lane runs on the task that
//! awaits it. Await it in place, or on a `tokio::task::LocalSet`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U256;
use alloy::providers::{DynProvider, Provider as _};
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context as _, bail};
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use iroh::{Endpoint, RelayUrl};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::discovery::NodeCandidate;
use crate::funding::RunFunding;
use crate::ordered_writes::OrderedWrites;
use crate::session::{
    ChainAdoption, ChainConfig, DepositPolicy, DiscoveryOptions, PinnedNode, PullSite,
};
use crate::stream_cap::LaneStreamCap;
use crate::{
    Connections, CredentialSlot, FundingNeeded, LaneLedgers, PeerHealth, PoolReplaced,
    ProgressClock, RecoveryGate, RunDemand, StopPolicy,
};

mod engine;
mod manifest_cache;
mod skip_cache;

#[doc(hidden)]
pub use crate::lane_sources::FaultContext;

/// The directory under the output root where a pull stages the blobs it
/// fetches: `<hex>.partial` and its resume record while a blob is in flight,
/// and `<hex>` once it is verified and before it lands at its paths.
pub const STAGING_DIR: &str = ".decdn-partial";

/// Parse a BLAKE3 hash as a bundle manifest or a user names it: 64 hex
/// characters, with an optional `0x` or `b3:` prefix.
///
/// # Errors
///
/// When `s` is not a 64-character hex digest.
pub fn parse_hash(s: &str) -> anyhow::Result<[u8; 32]> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("b3:"))
        .unwrap_or(s);
    let h = blake3::Hash::from_hex(hex).map_err(|e| {
        anyhow::anyhow!(
            "invalid hash {s:?}: expected 64 hex chars (BLAKE3 digest), optional `0x`/`b3:` \
             prefix: {e}"
        )
    })?;
    Ok(*h.as_bytes())
}

/// A bundle manifest: the files a bundle publishes (`adr/appendix-bundles.md`).
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct BundleManifest {
    /// Its files, in manifest order.
    pub entries: Vec<ManifestEntry>,
}

/// The wire form of a manifest: a version gate around the entries.
#[derive(Deserialize)]
struct ManifestV1 {
    version: u32,
    entries: Vec<ManifestEntry>,
}

impl BundleManifest {
    /// A manifest of `entries`.
    #[must_use]
    pub const fn new(entries: Vec<ManifestEntry>) -> Self {
        Self { entries }
    }

    /// Parse manifest JSON and check its version.
    ///
    /// # Errors
    ///
    /// When the bytes are not a manifest, or name a version other than 1.
    pub fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let manifest: ManifestV1 =
            serde_json::from_slice(bytes).context("parse bundle JSON (expected v1 manifest)")?;
        if manifest.version != 1 {
            bail!(
                "unsupported bundle version {} (this build supports v1)",
                manifest.version
            );
        }
        Ok(Self {
            entries: manifest.entries,
        })
    }

    /// The manifest blob `hash` a pull into `out_dir` cached, or `None` when
    /// none is cached. The cache is self-verifying: bytes that do not hash to
    /// `hash` are an error, as is an unreadable cache file.
    ///
    /// # Errors
    ///
    /// The cached file cannot be read, does not hash to `hash`, or is not a
    /// manifest.
    pub fn cached(out_dir: &Path, hash: [u8; 32]) -> anyhow::Result<Option<Self>> {
        manifest_cache::load(out_dir, hash)?
            .map(|bytes| {
                Self::parse(&bytes).with_context(|| {
                    format!(
                        "cached bundle manifest {}",
                        manifest_cache::cache_path(out_dir, hash).display()
                    )
                })
            })
            .transpose()
    }

    /// Where a pull into `out_dir` caches the manifest blob `hash`.
    #[must_use]
    pub fn cache_path(out_dir: &Path, hash: [u8; 32]) -> PathBuf {
        manifest_cache::cache_path(out_dir, hash)
    }
}

/// One file of a [`BundleManifest`].
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct ManifestEntry {
    /// The file's POSIX relative path under the output directory.
    pub path: String,
    /// The file's whole-file BLAKE3 hash (`b3:`hex), which the fetch verifies.
    pub hash: String,
    /// The file's size. Optional on the wire: the pull uses it as the first
    /// size claim and for progress totals, and verifies the true size.
    #[serde(default)]
    pub size: Option<u64>,
    /// The file's ordered chunk decomposition, a range-dedup hint. Chunks it
    /// shares with a sibling entry are spliced from disk instead of paid for
    /// twice. The whole file is still verified by `hash`.
    #[serde(default)]
    pub chunks: Option<Vec<ManifestChunk>>,
}

impl ManifestEntry {
    /// An entry for the file at `path` with whole-file `hash` and `size`, and
    /// no chunk hints.
    #[must_use]
    pub fn new(path: impl Into<String>, hash: impl Into<String>, size: Option<u64>) -> Self {
        Self {
            path: path.into(),
            hash: hash.into(),
            size,
            chunks: None,
        }
    }

    /// This entry with the chunk decomposition `chunks`.
    #[must_use]
    pub fn with_chunks(mut self, chunks: Vec<ManifestChunk>) -> Self {
        self.chunks = Some(chunks);
        self
    }
}

/// One chunk of a [`ManifestEntry`]: an independently BLAKE3-addressed run of
/// the file's bytes, placed at the running sum of the earlier chunks' sizes.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct ManifestChunk {
    /// The chunk's BLAKE3 hash (`b3:`hex).
    pub hash: String,
    /// The chunk's length in bytes. The sizes sum to the entry's size.
    pub size: u64,
}

impl ManifestChunk {
    /// A chunk of `size` bytes with BLAKE3 `hash`.
    #[must_use]
    pub fn new(hash: impl Into<String>, size: u64) -> Self {
        Self {
            hash: hash.into(),
            size,
        }
    }
}

/// What a bundle pull pays from.
#[derive(Debug)]
#[non_exhaustive]
pub enum Funding {
    /// The buyer's own pool: reused from the buyer store, adopted from chain,
    /// or opened at the working deposit, and topped up when it runs low.
    SelfOwned {
        /// The pool owner's key: it signs the vouchers and every pool
        /// transaction.
        signer: Arc<PrivateKeySigner>,
        /// How the pool is funded.
        deposit: DepositPolicy,
    },
    /// A pool another owner funds, under the capability the slot holds. The
    /// pull opens and tops up nothing; when the capability cannot pay for the
    /// rest, the pull ends [`FundingNeeded`].
    Delegated {
        /// The delegate key and the owner-signed capability.
        slot: CredentialSlot,
    },
}

/// Where a pull's entries find their holders: the pinned node, or every
/// active registered node for each entry to sample. Resolve it with
/// [`select_nodes`].
#[derive(Debug, Clone)]
pub struct NodeSelection {
    /// The node every entry pulls from, when one is pinned.
    pinned: Option<PinnedNode>,
    /// Every active registered node, when none is pinned.
    registry: Option<Vec<NodeCandidate>>,
}

/// Resolve where a pull's entries find their holders, once for the run.
///
/// With a pinned node every entry pulls from it. Otherwise the run's
/// candidate list is every active registered node: the peer store's
/// identity-fresh records when enough of them qualify, else one
/// `CapacityBond` registry read bounded by
/// [`DiscoveryOptions::registry_timeout`]. Each entry then probes its own
/// sample of the list. Resolve the selection before you unlock a key, so a
/// failed discovery costs no password prompt.
///
/// # Errors
///
/// No node is pinned and `chain` names no `CapacityBond`, or the registry
/// read fails with no usable peer store, or the registry lists no node.
pub async fn select_nodes(
    chain: &ChainConfig,
    site: &PullSite,
    discovery: &DiscoveryOptions,
) -> anyhow::Result<NodeSelection> {
    if let Some(pinned) = discovery.pinned {
        return Ok(NodeSelection {
            pinned: Some(pinned),
            registry: None,
        });
    }
    Ok(NodeSelection {
        pinned: None,
        registry: Some(engine::discover_candidates(chain, site, discovery).await?),
    })
}

/// Where a bundle pull reports its progress. Every method has a default
/// that does nothing. The pull calls them from the task that awaits it.
pub trait BundleObserver: Send + Sync {
    /// The pull starts on the entries it was given. `download_total` is the
    /// content bytes it plans to pay for, after shared chunks and duplicate
    /// files count once; `content_total` is the bytes that land on disk.
    /// Either is `None` when no entry declares a size.
    fn run_started(&self, download_total: Option<u64>, content_total: Option<u64>) {
        let _ = (download_total, content_total);
    }

    /// One blob starts: it lands at every one of `paths`. `download_total` is
    /// the bytes it plans to pay for, and `reconstruct_total` the bytes it
    /// plans to splice from disk. The returned observer follows that blob.
    fn file_started(
        &self,
        paths: &[String],
        download_total: u64,
        reconstruct_total: u64,
    ) -> Arc<dyn FileObserver> {
        let _ = (paths, download_total, reconstruct_total);
        Arc::new(NoFileObserver)
    }

    /// One blob landed in its staging file.
    fn entry_landed(&self, landed: &EntryLanded) {
        let _ = landed;
    }

    /// Something went wrong that does not fail the pull, such as a manifest
    /// cache file that cannot be read or written.
    fn warning(&self, warning: &str) {
        let _ = warning;
    }

    /// A funding recovery step replaced the pool. The pull runs the remaining
    /// entries once more against the new pool.
    fn pool_replaced(&self, replaced: &PoolReplaced) {
        let _ = replaced;
    }
}

/// Where one blob of a bundle pull reports its progress. Every method has a
/// default that does nothing.
pub trait FileObserver: Send + Sync {
    /// `received` of the blob's `expected` content bytes are verified. Every
    /// fetch of the blob reports its whole position.
    fn delivered(&self, received: u64, expected: u64) {
        let _ = (received, expected);
    }

    /// The blob probes its holders before any byte arrives.
    fn discovering(&self) {}

    /// The blob's own bytes are in, and it waits for a sibling to land the
    /// chunks it shares.
    fn pending(&self) {}

    /// The blob is assembled, and its whole-file hash runs.
    fn reconstructing(&self) {}

    /// The whole-file hash has read `bytes` bytes.
    fn hashed(&self, bytes: u64) {
        let _ = bytes;
    }

    /// The blob's pull settled, landed or failed.
    fn finished(&self) {}
}

/// The [`FileObserver`] that ignores everything.
struct NoFileObserver;

impl FileObserver for NoFileObserver {}

/// One blob that landed ([`BundleObserver::entry_landed`]). No byte counts in
/// more than one of `paid`, `spliced` and `resumed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EntryLanded {
    /// The blob's hash.
    pub hash: [u8; 32],
    /// The blob's size.
    pub size: u64,
    /// The content bytes this pull fetched and paid for.
    pub paid: u64,
    /// The bytes spliced from a file already on disk.
    pub spliced: u64,
    /// The bytes an earlier, interrupted pull fetched, which this pull resumed.
    pub resumed: u64,
    /// How long the blob took: probing, every fetch, any splice and the
    /// whole-file check.
    pub elapsed: Duration,
}

/// Explains a failed fetch to the caller's user, before the pull records the
/// error. The `decdn` CLI names its own flags and commands here.
#[doc(hidden)]
pub trait FaultHints: Send + Sync {
    /// `err` with what the user can do about it, given what the failed
    /// fetch's lanes saw.
    fn annotate(&self, err: anyhow::Error, at: &FaultContext) -> anyhow::Error;
}

/// How a bundle pull ended, and what it did.
#[derive(Debug)]
#[non_exhaustive]
pub struct BundleOutcome {
    /// What every entry did.
    pub report: BundleReport,
    /// Why the pull ended.
    pub end: BundleEnd,
    /// The pool replacement a funding recovery step made, if one did. The
    /// closed pool may still hold a balance to reclaim once its dispute
    /// window ends.
    pub replaced: Option<PoolReplaced>,
}

/// Why a bundle pull ended.
#[derive(Debug)]
#[non_exhaustive]
pub enum BundleEnd {
    /// Every entry settled. Some may have failed
    /// ([`BundleReport::failures`]); a rerun retries them.
    Complete,
    /// The cancel token fired. What landed is recorded, and a rerun resumes.
    Interrupted,
    /// The pool cannot pay for the rest: the capability is spent, expired or
    /// revoked, or the delegated pool is unfunded. `error` is the whole
    /// chain.
    FundingNeeded {
        /// What funding the pull needs.
        needed: FundingNeeded,
        /// The error the pull ended with.
        error: anyhow::Error,
    },
    /// A fault only the user can fix ended the pull, or it gave up after
    /// making no progress for its whole limit ([`crate::GaveUp`]).
    Stopped(anyhow::Error),
}

/// What a bundle pull did. Counts are per entry. Every byte total but
/// [`Self::paid_bytes`] is content bytes and leaves out the bao proof
/// overhead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BundleReport {
    /// Entries this pull fetched and paid for, one per distinct blob.
    pub fetched: u64,
    /// Entries linked or copied from a blob another path of this pull fetched.
    pub linked: u64,
    /// Entries already on disk.
    pub skipped: u64,
    /// Entries that failed ([`Self::failures`]).
    pub failed: u64,
    /// Distinct blobs linked from a byte-identical file already in the output
    /// directory, with no download.
    pub reused: u64,
    /// The size of those blobs, once each.
    pub reused_bytes: u64,
    /// The content bytes this pull fetched and paid for, from entry sizes: a
    /// blob counts once, less what it spliced or resumed.
    pub downloaded: u64,
    /// The lane bytes this pull's vouchers and hash-chain reveals paid for:
    /// how far it grew the claims the nodes can redeem, summed over every lane
    /// ([`crate::PoolLedger::paid_bytes`]). These are wire bytes, so they
    /// include the bao proof overhead. Content delivered twice, or paid for and
    /// then dropped after a lane fault, counts each time it is paid. An entry
    /// that fails or is interrupted counts here but not in `downloaded`; a
    /// later pull resumes its `.partial` and reports it as `resumed_bytes`.
    pub paid_bytes: u64,
    /// The bytes written to disk: every landed copy of every blob.
    pub reconstructed: u64,
    /// The bytes of `.partial` prefixes earlier pulls fetched, which this pull
    /// resumed. Never part of `downloaded`.
    pub resumed_bytes: u64,
    /// The bytes spliced from files already on disk. Never downloaded.
    pub spliced_bytes: u64,
    /// Range-dedup hints a fault dropped: a chunk list that does not parse or
    /// sum to the file size, or a donor that failed its check.
    pub hints_ignored: u64,
    /// Every failed entry, in manifest order.
    pub failures: Vec<EntryFailure>,
    /// Lines that do not fail the pull: a blob whose size differs from its
    /// manifest, or a wallet too short of USDC to top the pool up.
    pub warnings: Vec<String>,
}

/// One entry a bundle pull failed to land.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct EntryFailure {
    /// The entry's path.
    pub path: String,
    /// Why, with secrets redacted.
    pub error: String,
}

/// Pulls bundles into a directory over one paid run: one pool, one
/// connection per node, one progress clock.
///
/// Build it with [`Self::builder`]. One puller serves one run: a manifest
/// fetch and one [`Self::pull`], or several pulls that share the run's lanes.
pub struct BundlePuller {
    ctx: engine::PullCtx<DynProvider>,
}

impl std::fmt::Debug for BundlePuller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundlePuller")
            .field("chain", &self.ctx.chain)
            .field("self_address", &self.ctx.self_address)
            .finish_non_exhaustive()
    }
}

/// Builds a [`BundlePuller`] ([`BundlePuller::builder`]).
#[must_use]
pub struct BundlePullerBuilder {
    chain: ChainConfig,
    site: PullSite,
    endpoint: Endpoint,
    store: Arc<dyn BuyerPoolStore>,
    funding: Funding,
    discovery: DiscoveryOptions,
    nodes: NodeSelection,
    relays: Vec<RelayUrl>,
    namespace: Option<u64>,
    jobs: usize,
    max_lane_streams: usize,
    max_rate_per_mb: u64,
    max_blob_bytes: u64,
    interactive: bool,
    give_up_after: Option<Duration>,
    observer: Option<Arc<dyn BundleObserver>>,
    hints: Option<Arc<dyn FaultHints>>,
}

impl std::fmt::Debug for BundlePullerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundlePullerBuilder")
            .field("chain", &self.chain)
            .field("jobs", &self.jobs)
            .finish_non_exhaustive()
    }
}

impl BundlePuller {
    /// A builder for a pull on `chain`, keeping its peer store and scratch
    /// files in `site`, dialing from `endpoint`, recording its pool and lanes
    /// in `store`, and paying from `funding`. `discovery` and `nodes`
    /// ([`select_nodes`]) say where entries find their holders.
    pub fn builder(
        chain: ChainConfig,
        site: PullSite,
        endpoint: Endpoint,
        store: Arc<dyn BuyerPoolStore>,
        funding: Funding,
        discovery: DiscoveryOptions,
        nodes: NodeSelection,
    ) -> BundlePullerBuilder {
        BundlePullerBuilder {
            chain,
            site,
            endpoint,
            store,
            funding,
            discovery,
            nodes,
            relays: Vec::new(),
            namespace: None,
            jobs: 4,
            max_lane_streams: 4,
            max_rate_per_mb: 0,
            max_blob_bytes: 1 << 40,
            interactive: false,
            give_up_after: None,
            observer: None,
            hints: None,
        }
    }

    /// Fetch the manifest blob `hash`, and cache it in `out_dir`. With
    /// `use_cache`, a manifest an earlier pull cached there is read instead,
    /// and nothing is paid; a cache file that cannot be read is reported to
    /// the observer and fetched over.
    ///
    /// # Errors
    ///
    /// The fetch fails, or the bytes are not a manifest.
    pub async fn fetch_manifest(
        &self,
        hash: [u8; 32],
        out_dir: &Path,
        use_cache: bool,
    ) -> anyhow::Result<BundleManifest> {
        Box::pin(self.ctx.fetch_manifest(hash, out_dir, use_cache)).await
    }

    /// Pull every entry of `manifest` into `out_dir`. With `overwrite`, files
    /// already on disk are fetched again instead of skipped. A fired `cancel`
    /// stops every fetch where it is; what landed is recorded, and the next
    /// pull resumes.
    ///
    /// An entry that fails does not stop the others. A fault only the user
    /// can fix, or the stop policy, ends the pull ([`BundleEnd`]).
    pub async fn pull(
        &mut self,
        manifest: &BundleManifest,
        out_dir: &Path,
        overwrite: bool,
        cancel: &CancellationToken,
    ) -> BundleOutcome {
        Box::pin(self.ctx.run(&manifest.entries, out_dir, overwrite, cancel)).await
    }

    /// The warning for a wallet seen to hold too little USDC to top the pool
    /// up during this run, if one was.
    #[must_use]
    pub fn funding_shortfall(&self) -> Option<String> {
        self.ctx.funding.shortfall()
    }

    /// Wait for every queued peer-record and watermark write to land. Call it
    /// before the process exits, after a cancelled pull too, so the next run
    /// resumes each lane from what it paid.
    pub async fn finish(self) {
        self.ctx.writes.settle().await;
    }
}

impl BundlePullerBuilder {
    /// The relays to hint when dialing a node. The first one rides each dial.
    pub fn relays(mut self, relays: Vec<RelayUrl>) -> Self {
        self.relays = relays;
        self
    }

    /// The namespace (ADR 002) every request carries: a node routes a cache
    /// miss to that namespace's origins. `None` is the best-effort default.
    pub const fn namespace(mut self, namespace: Option<u64>) -> Self {
        self.namespace = namespace;
        self
    }

    /// How many blobs fetch at once (at least 1; 4 by default).
    pub const fn jobs(mut self, jobs: usize) -> Self {
        self.jobs = jobs;
        self
    }

    /// How many streams one provider's lane runs at once across the whole
    /// pull (at least 1; 4 by default).
    pub const fn max_lane_streams(mut self, n: usize) -> Self {
        self.max_lane_streams = n;
        self
    }

    /// The highest per-MB rate a lane accepts; `0`, the default, sets no
    /// ceiling.
    pub const fn max_rate_per_mb(mut self, rate: u64) -> Self {
        self.max_rate_per_mb = rate;
        self
    }

    /// The largest blob one entry fetches (1 TiB by default).
    pub const fn max_blob_bytes(mut self, bytes: u64) -> Self {
        self.max_blob_bytes = bytes;
        self
    }

    /// When the pull gives up after making no progress
    /// ([`crate::StopPolicy`]): an `interactive` pull waits for its caller to
    /// cancel, and any other stops after 10 minutes. `give_up_after`
    /// overrides both. Non-interactive by default.
    pub const fn stop(mut self, interactive: bool, give_up_after: Option<Duration>) -> Self {
        self.interactive = interactive;
        self.give_up_after = give_up_after;
        self
    }

    /// Where the pull reports its progress. None by default.
    pub fn observer(mut self, observer: Arc<dyn BundleObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// What explains a failed fetch to the caller's user.
    #[doc(hidden)]
    pub fn fault_hints(mut self, hints: Arc<dyn FaultHints>) -> Self {
        self.hints = Some(hints);
        self
    }

    /// Build the puller: connect to the RPC endpoint and read the pool's
    /// settlement token.
    ///
    /// # Errors
    ///
    /// The RPC URL is malformed, the capability does not decode, or the
    /// `PaymentPool` token read fails.
    pub async fn build(self) -> anyhow::Result<BundlePuller> {
        let (signer, slot, deposit) = match self.funding {
            Funding::SelfOwned { signer, deposit } => (signer, None, deposit),
            Funding::Delegated { slot } => {
                // A delegate owns no pool, so it funds and adopts none. Its
                // lanes sign under the slot's credential as each is built; the
                // provider below sends no transaction.
                let deposit = DepositPolicy {
                    working_deposit: U256::ZERO,
                    max_approve: false,
                    adoption: ChainAdoption::Refused,
                };
                (slot.current().signer, Some(slot), deposit)
            }
        };
        let rpc = crate::provider::build_provider(&self.chain.rpc_url, &signer)?.erased();
        let contract = PaymentPool::new(self.chain.payment_pool, rpc.clone());
        let token = contract
            .usdc()
            .call()
            .await
            .map_err(|e| anyhow::Error::new(e).context("read PaymentPool.usdc()"))?;
        let ctx = engine::PullCtx {
            voucher_dom: voucher_domain(self.chain.chain_id, self.chain.payment_pool),
            slash_dom: slash_judge_domain(self.chain.chain_id, self.chain.slash_judge),
            connections: Connections::new(self.endpoint.clone()),
            self_address: signer.address(),
            endpoint: self.endpoint,
            store: self.store,
            contract,
            rpc,
            signer,
            token,
            chain: self.chain,
            site: self.site,
            relays: self.relays,
            nodes: self.nodes,
            discovery: self.discovery,
            max_rate_per_mb: self.max_rate_per_mb,
            max_blob_bytes: self.max_blob_bytes,
            deposit,
            slot,
            namespace_id: namespace_id(self.namespace),
            ledgers: LaneLedgers::new(),
            funding: RunFunding::default(),
            recovery: Arc::new(RecoveryGate::new()),
            dedup_stats: engine::DedupStats::default(),
            open_lock: tokio::sync::Mutex::new(()),
            gate: engine::JobGate::new(self.jobs.max(1)),
            lane_cap: LaneStreamCap::new(self.max_lane_streams),
            health: Arc::new(PeerHealth::default()),
            writes: OrderedWrites::default(),
            // One clock for the whole run: a stuck entry waits while another
            // lands bytes.
            stop: StopPolicy::new(
                self.interactive,
                self.give_up_after,
                Arc::new(ProgressClock::new()),
            ),
            demand: Arc::new(RunDemand::default()),
            observer: self.observer,
            hints: self.hints,
        };
        Ok(BundlePuller { ctx })
    }
}

/// The namespace id (ADR 002) every paid pull in the run carries: `namespace`
/// as a big-endian `uint256`, or `NO_NAMESPACE` when it is `None`.
fn namespace_id(namespace: Option<u64>) -> [u8; 32] {
    namespace.map_or(decdn_protocol::client::NO_NAMESPACE, |n| {
        U256::from(n).to_be_bytes()
    })
}
