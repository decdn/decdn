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
//! 3. Get the manifest: [`BundlePuller::fetch_manifest`] it by its hash, or
//!    [`BundleManifest::parse`] bytes you already hold.
//! 4. Drop the entries you do not want, then [`BundlePuller::pull`] the rest.
//!    The puller writes every entry path the path-safety rules accept, hidden
//!    (`.`-prefixed) components included, except its own state paths: when
//!    the user did not choose the output directory, vet the entry paths
//!    first, as the `decdn get` CLI does. Progress reaches a [`BundleObserver`]; the result is a
//!    [`BundleOutcome`].
//! 5. [`BundlePuller::finish`] waits for the lanes' watermark writes.
//!
//! The pull future is not `Send`: every entry and lane runs on the task that
//! awaits it. Await it in place, or on a `tokio::task::LocalSet`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U256;
use alloy::providers::{DynProvider, Provider as _};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context as _;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{slash_judge_domain, voucher_domain};
use iroh::{Endpoint, RelayUrl};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::buyer_pool::{OwnedPayer, UnconfirmedEscrows};
use crate::funding::RunFunding;
use crate::ordered_writes::OrderedWrites;
use crate::receipt::ReceiptEvidence;
use crate::session::{
    ChainAdoption, ChainConfig, DepositPolicy, DiscoveryOptions, PinnedNode, PoolOpen, PullSite,
};
use crate::stream_cap::LaneStreamCap;
use crate::uri::DecdnUri;
use crate::{
    Connections, CredentialSlot, FundingNeeded, LaneLedgers, PeerHealth, PoolReplaced,
    ProgressClock, RecoveryGate, RunDemand, StopPolicy,
};

mod engine;
mod landing;
mod manifest_cache;
mod skip_cache;
#[cfg(test)]
mod tests;

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

/// A BLAKE3 hash in the form a bundle manifest writes it: `b3:` and 64
/// lowercase hex characters.
#[must_use]
pub fn format_hash(hash: [u8; 32]) -> String {
    format!("b3:{}", blake3::Hash::from_bytes(hash).to_hex())
}

/// A bundle manifest: the files a bundle publishes (`adr/appendix-bundles.md`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BundleManifest {
    /// The manifest blob's BLAKE3 hash: the hash a `decdn:<hash>/` URI names.
    hash: [u8; 32],
    /// Its files, in manifest order.
    pub entries: Vec<ManifestEntry>,
}

/// The wire form of a manifest: a version gate around the entries, whose
/// hashes parse only once the version is known.
#[derive(Deserialize)]
struct ManifestV1 {
    version: u32,
    entries: Vec<WireEntry>,
}

impl BundleManifest {
    /// A manifest of `entries` whose blob has the BLAKE3 hash `hash`. A pull
    /// records `hash` as the source of each file it writes, so a wrong one
    /// makes a later pull of the same entry see its own files as foreign.
    ///
    /// Each entry `path` must name its own file ([`path_collision`]): a pull
    /// fails every entry whose path names the same file as another's, and
    /// writes none of them. A pull also fails an entry whose path holds a
    /// segment [`path_segment_problem`] refuses. [`BundleManifest::parse`]
    /// rejects such a manifest outright.
    #[must_use]
    pub const fn new(hash: [u8; 32], entries: Vec<ManifestEntry>) -> Self {
        Self { hash, entries }
    }

    /// The manifest blob's BLAKE3 hash. A pull records it as the source of
    /// each file it writes. Filtering `entries` keeps it: it names the whole
    /// manifest.
    #[must_use]
    pub const fn hash(&self) -> [u8; 32] {
        self.hash
    }

    /// Parse manifest JSON, check its version, then parse every hash.
    ///
    /// # Errors
    ///
    /// [`NotAManifest`] when the bytes are not a manifest or name a version
    /// other than 1. [`MalformedManifest`] when a v1 manifest holds an entry
    /// or chunk hash that does not parse, an entry path with a segment that is
    /// not a portable file name ([`path_segment_problem`]), or two entries
    /// whose paths name one file on a case- or normalization-insensitive file
    /// system ([`path_collision`]). One bad entry makes the whole manifest
    /// invalid. A retry cannot fix either error: the bytes are
    /// content-addressed, so every read of them gets the same answer.
    pub fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let manifest: ManifestV1 = serde_json::from_slice(bytes)
            .map_err(|e| NotAManifest(format!("parse bundle JSON (expected v1 manifest): {e}")))?;
        if manifest.version != 1 {
            return Err(NotAManifest(format!(
                "unsupported bundle version {} (this build supports v1)",
                manifest.version
            ))
            .into());
        }
        let entries: Vec<ManifestEntry> = manifest
            .entries
            .into_iter()
            .map(WireEntry::parse)
            .collect::<Result<_, _>>()?;
        if let Some((first, second)) = path_collision(entries.iter().map(|en| en.path.as_str())) {
            return Err(MalformedManifest(format!(
                "two bundle entries name the same file on a case- or normalization-insensitive \
                 file system: {first:?} and {second:?}"
            ))
            .into());
        }
        Ok(Self {
            hash: *blake3::hash(bytes).as_bytes(),
            entries,
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

/// The key under which two entry paths name the same file on a file system
/// that ignores case or Unicode normalization (by default, macOS ignores both
/// and Windows ignores case), close to the canonical caseless match of Unicode
/// § 3.13. The path is decomposed (NFD), folded to lower case, then upper
/// case, then lower case again, and composed (NFC). The fold joins `ẞ`, `ß` and `ss`, as full case
/// folding does. It also joins a few pairs full case folding keeps apart, such
/// as dotless `ı` and `i`; such a pair is refused, not written over.
fn path_key(path: &str) -> String {
    let decomposed = icu_normalizer::DecomposingNormalizerBorrowed::new_nfd().normalize(path);
    let folded = decomposed.to_lowercase().to_uppercase().to_lowercase();
    icu_normalizer::ComposingNormalizerBorrowed::new_nfc()
        .normalize(&folded)
        .into_owned()
}

/// Two of `paths` that name the same file on a file system that ignores case
/// or Unicode normalization: the earliest path whose key repeats an earlier
/// path's, paired with that earlier path. `None` when every path names its own
/// file. The key is NFD, then lower, upper and lower case, then NFC, so
/// `README` and `readme`, `straße` and `STRASSE`, and a composed and a
/// decomposed `é` each collide.
///
/// [`BundleManifest::parse`] rejects a manifest with such a pair, and a pull
/// fails every entry of one. A publisher checks its paths with it before it
/// writes anything.
pub fn path_collision<'a>(paths: impl IntoIterator<Item = &'a str>) -> Option<(&'a str, &'a str)> {
    let mut seen: HashMap<String, &'a str> = HashMap::new();
    paths
        .into_iter()
        .find_map(|path| seen.insert(path_key(path), path).map(|first| (first, path)))
}

/// Why `segment`, one `/`-separated segment of a bundle entry path, is not a
/// file name that names one file on every supported platform, or `None` when
/// it is (`appendix-bundles.md` § Path-safety rules, rules 2 and 8).
///
/// Refused on every platform, so a manifest that parses on one reads the same
/// on all of them:
/// - an empty segment, `.` or `..`;
/// - a control character (C0, DEL or C1), which a terminal that prints the
///   path would interpret;
/// - a character Windows reserves: `\ : * ? " < > |` (`:` names an NTFS
///   stream, as in `README::$DATA`);
/// - a trailing `.` or space, which Windows strips, so `README.` and
///   `README ` name `README`;
/// - a Windows device name as the stem (`CON`, `PRN`, `AUX`, `NUL`, `COM0`–`9`,
///   `LPT0`–`9`, and `COM` / `LPT` with a superscript `¹`, `²` or `³`, in any
///   case and with any extension);
/// - a stem that ends in `~` and digits, the form of an 8.3 short name
///   (`LONGFI~1.TXT`), which Windows resolves to whichever long name it was
///   generated for.
#[must_use]
pub fn path_segment_problem(segment: &str) -> Option<&'static str> {
    if segment.is_empty() {
        return Some("empty segment");
    }
    if segment == "." || segment == ".." {
        return Some("relative-directory segment");
    }
    if segment.chars().any(char::is_control) {
        return Some("control character");
    }
    if segment.contains(['\\', ':', '*', '?', '"', '<', '>', '|']) {
        return Some("character Windows reserves");
    }
    if segment.ends_with(['.', ' ']) {
        return Some("trailing dot or space");
    }
    // Windows also strips spaces before the extension: `CON .txt` is `CON`.
    let stem = segment
        .split('.')
        .next()
        .unwrap_or(segment)
        .trim_end_matches(' ');
    if is_device_name(stem) {
        return Some("Windows device name");
    }
    if let Some((base, digits)) = stem.rsplit_once('~')
        && !base.is_empty()
        && !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
    {
        return Some("8.3 short-name form");
    }
    None
}

/// Whether `stem` is a Windows device name, in any case: `CON`, `PRN`, `AUX`,
/// `NUL`, or `COM` / `LPT` and one digit, superscript `¹`, `²` and `³` included.
fn is_device_name(stem: &str) -> bool {
    let upper = stem.to_ascii_uppercase();
    if matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL") {
        return true;
    }
    let Some(port) = upper
        .strip_prefix("COM")
        .or_else(|| upper.strip_prefix("LPT"))
    else {
        return false;
    };
    let mut chars = port.chars();
    matches!(
        (chars.next(), chars.next()),
        (Some('0'..='9' | '\u{b9}' | '\u{b2}' | '\u{b3}'), None)
    )
}

/// The entries of `manifest` whose path under `out_dir` holds something that
/// a pull of the same entry may replace without the user's say: a file,
/// directory or link that no record in the skip-cache in `out_dir` vouches
/// for. A record vouches for a path when the file there still has the
/// recorded size and mtime, and the record names this entry (this manifest
/// hash and manifest path) or holds this entry's bytes. A caller that did not
/// choose `out_dir` can refuse the pull first.
///
/// An entry whose path escapes `out_dir`, or that lands on the pull's own
/// state, is not named: the pull fails that entry and writes nothing there.
#[must_use]
pub fn foreign_paths(manifest: &BundleManifest, out_dir: &Path) -> Vec<ForeignPath> {
    let saved = skip_cache::load_checked(out_dir);
    manifest
        .entries
        .iter()
        .filter_map(|en| {
            let dest = engine::writable_dest(out_dir, &en.path).ok()?;
            let meta = std::fs::symlink_metadata(dest).ok()?;
            let reason = match &saved {
                Err(e) => ForeignReason::UnreadableCache(format!("{e:#}")),
                Ok(saved) => match saved.get(&en.path) {
                    None => ForeignReason::Unrecorded,
                    Some(rec) if !rec.still_matches(&meta) => {
                        ForeignReason::Changed(rec.source.uri())
                    }
                    Some(rec)
                        if rec.source.is(manifest.hash, en.manifest_path())
                            || rec.holds(en.hash) =>
                    {
                        return None;
                    }
                    Some(rec) => ForeignReason::OtherSource(rec.source.uri()),
                },
            };
            Some(ForeignPath {
                path: en.path.clone(),
                reason,
            })
        })
        .collect()
}

/// A path a pull may replace that no record vouches for ([`foreign_paths`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ForeignPath {
    /// The entry's path under the output directory.
    pub path: String,
    /// Why no record vouches for `path`.
    pub reason: ForeignReason,
}

/// Why no skip-cache record vouches for a [`ForeignPath`].
///
/// A held [`DecdnUri`] is the bundle entry an earlier pull wrote the path
/// from. It is `None` when the record names no readable manifest hash.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ForeignReason {
    /// No pull recorded writing the path.
    Unrecorded,
    /// The skip-cache file is there, but it cannot be used: it cannot be
    /// read, it is not a skip-cache, or another build wrote it. Holds the
    /// cause.
    UnreadableCache(String),
    /// A pull wrote the path, and it changed after: its size, its mtime, or
    /// its kind differs from the record.
    Changed(Option<DecdnUri>),
    /// A pull wrote the path from another bundle entry, with other bytes.
    OtherSource(Option<DecdnUri>),
}

/// Bytes that are not a v1 bundle manifest: [`BundleManifest::parse`]'s error,
/// so a caller can tell a blob that is not a manifest from a failed fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotAManifest(String);

impl std::fmt::Display for NotAManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NotAManifest {}

/// A v1 bundle manifest with an entry or chunk hash that does not parse, an
/// entry path segment that is not a portable file name
/// ([`path_segment_problem`]), or two entry paths that name one file
/// ([`path_collision`]):
/// [`BundleManifest::parse`]'s error for a manifest its publisher wrote wrong.
/// Fetching the raw blob only returns the same manifest bytes; only a fixed
/// manifest, under a new hash, serves the entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MalformedManifest(String);

impl std::fmt::Display for MalformedManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MalformedManifest {}

/// One file of a [`BundleManifest`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ManifestEntry {
    /// The file's POSIX relative path under the output directory.
    pub path: String,
    /// The file's whole-file BLAKE3 hash, which the fetch verifies.
    pub hash: [u8; 32],
    /// The file's size. Optional on the wire: the pull uses it as the first
    /// size claim and for progress totals, and verifies the true size.
    pub size: Option<u64>,
    /// The file's ordered chunk decomposition, a range-dedup hint. Chunks it
    /// shares with a sibling entry are spliced from disk instead of paid for
    /// twice. The whole file is still verified by `hash`.
    pub chunks: Option<Vec<ManifestChunk>>,
    /// The manifest `path` of an entry that [`Self::land_at`] moved, or `None`
    /// when `path` is still the manifest path.
    landed_from: Option<String>,
}

/// The wire form of a [`ManifestEntry`]: its hashes as the manifest writes
/// them, before they parse.
#[derive(Deserialize)]
struct WireEntry {
    path: String,
    hash: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    chunks: Option<Vec<WireChunk>>,
}

/// The wire form of a [`ManifestChunk`].
#[derive(Deserialize)]
struct WireChunk {
    hash: String,
    size: u64,
}

impl WireEntry {
    /// This entry with its hashes parsed, or the error naming the entry and
    /// the first hash that does not parse.
    fn parse(self) -> Result<ManifestEntry, MalformedManifest> {
        let Self {
            path,
            hash,
            size,
            chunks,
        } = self;
        let malformed = |what: &str, e: anyhow::Error| {
            MalformedManifest(format!(
                "bundle entry {path:?} has a malformed {what}: {e:#}"
            ))
        };
        if let Some((segment, why)) = path
            .split('/')
            .find_map(|seg| path_segment_problem(seg).map(|why| (seg, why)))
        {
            return Err(MalformedManifest(format!(
                "bundle entry {path:?} has an unsafe path segment {segment:?}: {why}"
            )));
        }
        let hash = parse_hash(&hash).map_err(|e| malformed("hash", e))?;
        let chunks = chunks
            .map(|chunks| {
                chunks
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| {
                        parse_hash(&c.hash)
                            .map(|hash| ManifestChunk { hash, size: c.size })
                            .map_err(|e| malformed(&format!("chunk {i} hash"), e))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        Ok(ManifestEntry {
            path,
            hash,
            size,
            chunks,
            landed_from: None,
        })
    }
}

impl ManifestEntry {
    /// An entry for the file at `path` with whole-file `hash` and `size`, and
    /// no chunk hints.
    #[must_use]
    pub fn new(path: impl Into<String>, hash: [u8; 32], size: Option<u64>) -> Self {
        Self {
            path: path.into(),
            hash,
            size,
            chunks: None,
            landed_from: None,
        }
    }

    /// This entry, landing at `path` under the output directory in place of
    /// its manifest path. The pull records the manifest path as the source of
    /// the file it writes. A second `land_at` keeps the first manifest path.
    /// Setting the `path` field directly makes the pull record that path as
    /// the manifest path. A pull fails every entry that lands at the same file
    /// as another ([`path_collision`]).
    #[must_use]
    pub fn land_at(mut self, path: impl Into<String>) -> Self {
        let manifest_path = std::mem::replace(&mut self.path, path.into());
        self.landed_from.get_or_insert(manifest_path);
        self
    }

    /// The entry's `path` in its manifest: where it lands, unless
    /// [`Self::land_at`] moved it.
    #[must_use]
    pub fn manifest_path(&self) -> &str {
        self.landed_from.as_deref().unwrap_or(&self.path)
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
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ManifestChunk {
    /// The chunk's BLAKE3 hash.
    pub hash: [u8; 32],
    /// The chunk's length in bytes. The sizes sum to the entry's size.
    pub size: u64,
}

impl ManifestChunk {
    /// A chunk of `size` bytes with BLAKE3 `hash`.
    #[must_use]
    pub const fn new(hash: [u8; 32], size: u64) -> Self {
        Self { hash, size }
    }
}

/// What a bundle pull pays from.
#[derive(Debug)]
#[non_exhaustive]
pub enum Funding {
    /// The buyer's own pool: reused from the buyer store, adopted from chain,
    /// or opened at the working deposit, and topped up when it runs low.
    SelfOwned {
        /// The pool owner's key: it signs every pool transaction and the
        /// capability for the lane key. The vouchers sign under a lane key
        /// derived from it and the buyer store's salt
        /// ([`crate::buyer_pool::OwnedPayer::for_store`]), so two stores on one
        /// owner sign on disjoint lanes.
        owner: Arc<PrivateKeySigner>,
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
    /// The pinned node or the registry list.
    selection: Selection,
}

impl NodeSelection {
    /// `discovery` with its pinned node taken from this selection, so an
    /// entry's first holders, its rediscovery and a pinned node's dial address
    /// all follow one node.
    const fn applied_to(&self, mut discovery: DiscoveryOptions) -> DiscoveryOptions {
        discovery.pinned = match &self.selection {
            Selection::Pinned(pinned) => Some(*pinned),
            Selection::Registry(_) => None,
        };
        discovery
    }
}

/// Where every entry of a run finds its holders, resolved once by
/// [`select_nodes`].
#[derive(Debug, Clone)]
enum Selection {
    /// The node every entry pulls from.
    Pinned(PinnedNode),
    /// Every active registered node, and whether the list is the
    /// registry-outage fallback.
    Registry(engine::Registry),
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
            selection: Selection::Pinned(pinned),
        });
    }
    Ok(NodeSelection {
        selection: Selection::Registry(engine::discover_candidates(chain, site, discovery).await?),
    })
}

/// The unpinned run's candidate list of [`select_nodes`], and whether it is
/// the registry-outage fallback, which the region allowlist leaves alone.
pub(crate) async fn registry_nodes(
    chain: &ChainConfig,
    site: &PullSite,
    discovery: &DiscoveryOptions,
) -> anyhow::Result<(Vec<crate::discovery::NodeCandidate>, bool)> {
    let registry = engine::discover_candidates(chain, site, discovery).await?;
    Ok((registry.nodes, registry.outage_fallback))
}

/// One distinct blob of a manifest and the content bytes a fresh pull of
/// it pays for ([`planned_downloads`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlannedBlob {
    /// The blob's hash.
    pub hash: [u8; 32],
    /// The blob's size as its manifest entries declare it; `None` when none
    /// does.
    pub size: Option<u64>,
    /// The content bytes a pull fetches for it: its whole size, less the
    /// chunks it shares with a smaller entry, which that entry fetches. At
    /// most `size`, and `0` for a blob with no declared size.
    pub download: u64,
    /// The wire bytes that download pays for: each range it fetches, with
    /// the bao proof path from the blob's own tree, as the nodes meter it
    /// (ADR 038 § Payment metering). `0` for a blob with no declared size.
    pub wire: u64,
}

/// Each distinct blob of `entries`, in first-seen order, with the bytes a
/// fresh pull of them all pays for. A blob two paths name counts once, and
/// so does a chunk two blobs share, under the plan a pull runs
/// ([`BundleObserver::run_started`] reports the same total).
#[must_use]
pub fn planned_downloads(entries: &[ManifestEntry]) -> Vec<PlannedBlob> {
    engine::planned_downloads(entries)
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
    /// Every pool replacement this pull's funding recovery steps made, in the
    /// order they made them. Each closed pool may still hold a balance to
    /// reclaim once its dispute window ends. A replacement that ended the
    /// first pass runs the rest again against the new pool. One a sibling
    /// entry's step made while another entry ended the pull, or one that an
    /// untracked escrow ended the pull over, runs nothing again, and is still
    /// listed here. A second replacement, in the pass against the new pool,
    /// runs nothing again and is listed after the first: the pool the first
    /// one opened is the one it closed. One that ends that pass ends the pull
    /// "funding needed". [`BundlePuller::replacements`] also lists a manifest
    /// fetch's.
    pub replaced: Vec<PoolReplaced>,
}

/// Why a bundle pull ended.
#[derive(Debug)]
#[non_exhaustive]
pub enum BundleEnd {
    /// Every entry settled. Some may have failed
    /// ([`BundleReport::failures`]); a rerun retries them.
    Complete,
    /// The cancel token fired. What landed is recorded, and a rerun resumes.
    /// The caller names what the puller's handle
    /// ([`BundlePuller::unconfirmed_escrows`]) still holds: each pending
    /// `openPool` and `topUp` ([`UnconfirmedEscrows::pending`]) and each
    /// untracked escrow ([`UnconfirmedEscrows::untracked`]). A rerun before
    /// those resolve escrows again.
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
    /// making no progress for its whole limit ([`crate::GaveUp`]). A pull that
    /// is not interrupted ends here while its handle holds an untracked escrow
    /// ([`UnconfirmedEscrows::untracked`]) or a pending `openPool` or `topUp`
    /// ([`UnconfirmedEscrows::pending`]), with an error that names each tx:
    /// an entry dropped while its receipt read was pending leaves one there.
    /// This includes every later pull on the same handle.
    Stopped(anyhow::Error),
}

/// What a bundle pull did. Counts are per entry, and `fetched`, `linked`,
/// `skipped`, `reused`, `failed` and `not_run` add up to the manifest's entry
/// count. Every byte total but
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
    /// Entries this pull did not run: another entry ended the pull, or the
    /// cancel token fired, before their fetch ended. A rerun runs them.
    pub not_run: u64,
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
    /// Range-dedup hints a fault dropped: a chunk list with no file size to
    /// check it against, or whose sizes do not sum to it, or a donor that
    /// failed its check.
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
    escrows: UnconfirmedEscrows,
    evidence: Option<Arc<ReceiptEvidence>>,
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
    /// ([`select_nodes`]) say where entries find their holders. `nodes`
    /// decides whether the run is pinned and to which node: it replaces
    /// `discovery.pinned`.
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
            escrows: UnconfirmedEscrows::default(),
            evidence: None,
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
    /// The puller writes every entry path the path-safety rules accept,
    /// hidden (`.`-prefixed) components included, except its own state
    /// paths. When the user did not choose `out_dir`, vetting the entry
    /// paths first is the caller's job — the `decdn get` CLI refuses hidden
    /// paths into a default destination.
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
        Box::pin(self.ctx.run(manifest, out_dir, overwrite, cancel)).await
    }

    /// Every pool replacement this puller's funding recovery steps made, in
    /// order: its manifest fetches' and its pulls' alike, and a step in a
    /// pull its caller dropped or cancelled. Each closed pool may still hold
    /// a balance to reclaim once its dispute window ends. A replacement that
    /// ended a manifest fetch is in no [`BundleOutcome`].
    #[must_use]
    pub fn replacements(&self) -> Vec<PoolReplaced> {
        self.ctx.recovery.replacements()
    }

    /// The warning for a wallet seen to hold too little USDC to top the pool
    /// up during this run, if one was.
    #[must_use]
    pub fn funding_shortfall(&self) -> Option<String> {
        self.ctx.funding.shortfall()
    }

    /// The handle that holds each `openPool` and `topUp` of the puller that no
    /// local record credits yet, and each that escrowed with no local record
    /// crediting it: the one passed to
    /// [`BundlePullerBuilder::unconfirmed_escrows`], or the puller's own.
    #[must_use]
    pub const fn unconfirmed_escrows(&self) -> &UnconfirmedEscrows {
        self.ctx.funding.escrows()
    }

    /// The rates the run's nodes signed, nearest first: from the probes this
    /// puller's fetches ran, else from the peer store. A node with neither is
    /// left out. `decdn get` prints its estimate from these before the pull
    /// starts (#2600).
    #[doc(hidden)]
    #[must_use]
    pub fn rate_hints(&self) -> Vec<crate::quote::RateHint> {
        self.ctx.rate_hints()
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

    /// Where the pull holds each `openPool` and `topUp` that no local record
    /// credits yet ([`UnconfirmedEscrows::pending`]), across every pass. A
    /// cancel or a fault that ends the pull can drop a receipt wait, and the
    /// transaction may still escrow. A pull that is not interrupted ends
    /// [`BundleEnd::Stopped`] on each one, and the caller keeps a clone to
    /// name what is left on [`BundleEnd::Interrupted`]. A `topUp` the handle
    /// holds also refuses every later `topUp` of the pull, across passes. The
    /// handle also holds each `openPool` and `topUp` that escrowed with no
    /// local record crediting it ([`UnconfirmedEscrows::untracked`]). It
    /// refuses every later `openPool` and `topUp`, ends every later pull on
    /// the handle [`BundleEnd::Stopped`], and is the caller's to name on
    /// [`BundleEnd::Interrupted`]. A private handle by default
    /// ([`BundlePuller::unconfirmed_escrows`]).
    pub fn unconfirmed_escrows(mut self, escrows: UnconfirmedEscrows) -> Self {
        self.escrows = escrows;
        self
    }

    /// Where every lane of the run keeps the signed messages a pull receipt
    /// holds ([`ReceiptEvidence::lanes`]). Nothing is kept by default.
    pub fn receipt_evidence(mut self, evidence: Arc<ReceiptEvidence>) -> Self {
        self.evidence = Some(evidence);
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
    /// The RPC URL is malformed, the capability does not decode, the buyer
    /// store cannot read or record its lane salt (a self-owned run), or the
    /// `PaymentPool` token read fails.
    pub async fn build(self) -> anyhow::Result<BundlePuller> {
        let (signer, payer, deposit) = match self.funding {
            Funding::SelfOwned { owner, deposit } => {
                let payer = OwnedPayer::for_store(Arc::clone(&owner), self.store.as_ref())?;
                (owner, engine::RunPayer::Owned(payer), deposit)
            }
            Funding::Delegated { slot } => {
                // A delegate owns no pool, so it funds and adopts none. Its
                // lanes sign under the slot's credential as each is built; the
                // provider below sends no transaction.
                let deposit = DepositPolicy {
                    working_deposit: U256::ZERO,
                    max_approve: false,
                    adoption: ChainAdoption::Refused,
                    open: PoolOpen::Refused,
                };
                (
                    slot.current().signer,
                    engine::RunPayer::Delegated(slot),
                    deposit,
                )
            }
        };
        let rpc = crate::provider::build_provider(&self.chain.rpc_url, &signer)?.erased();
        let contract = PaymentPool::new(self.chain.payment_pool, rpc.clone());
        let token = contract
            .usdc()
            .call()
            .await
            .map_err(|e| anyhow::Error::new(e).context("read PaymentPool.usdc()"))?;
        let discovery = self.nodes.applied_to(self.discovery);
        let ctx = engine::PullCtx {
            voucher_dom: voucher_domain(self.chain.chain_id, self.chain.payment_pool),
            slash_dom: slash_judge_domain(self.chain.chain_id, self.chain.slash_judge),
            connections: Connections::new(self.endpoint.clone()),
            self_address: signer.address(),
            endpoint: self.endpoint,
            store: self.store,
            contract,
            rpc,
            payer,
            token,
            chain: self.chain,
            site: self.site,
            relays: self.relays,
            nodes: self.nodes,
            discovery,
            max_rate_per_mb: self.max_rate_per_mb,
            max_blob_bytes: self.max_blob_bytes,
            deposit,
            namespace_id: namespace_id(self.namespace),
            ledgers: LaneLedgers::new(),
            funding: RunFunding::reporting_escrows_to(self.escrows),
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
            evidence: self.evidence,
            rate_samples: std::sync::Mutex::default(),
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
