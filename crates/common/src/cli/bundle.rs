//! Arguments for the `decdn bundle` subcommand group (issue #391).
//!
//! Bundles are a publisher-side convenience for grouping multiple
//! BLAKE3-content-addressed blobs into a single JSON manifest file. The
//! manifest itself is also a blob (BLAKE3-hashed), so origins can publish
//! a single bundle hash and clients fetch the bundle then everything it
//! references. See [`appendix-bundles`](../../../adr/appendix-bundles.md)
//! for the on-disk schema, hash format (`b3:<hex>`), path-safety rules,
//! and the determinism requirement that makes bundle-as-blob distribution
//! viable.
//!
//! `origin import --dry-run` produces manifests; `bundle pull` (#391)
//! realizes them — fetching every referenced blob over the paid
//! `cdn/client/v1` path (`decdn fetch`'s primitive), with per-entry node
//! discovery and bounded concurrency.

use std::path::PathBuf;

use clap::{ArgGroup, Args, Subcommand};

use super::fetch::ClientFetchArgs;

/// Top-level `decdn bundle` group.
#[derive(Args, Debug)]
pub struct BundleArgs {
    /// The `decdn bundle` subcommand to run.
    #[command(subcommand)]
    pub cmd: BundleCommand,
}

/// Subcommands under `decdn bundle`.
#[derive(Subcommand, Debug)]
pub enum BundleCommand {
    /// Fetch every blob a bundle references into an output directory over the
    /// paid `cdn/client/v1` path (#391). The bundle is read from a local file
    /// (`-i`) or fetched first by its own hash (`--hash`); each entry is then
    /// discovered + fetched independently, with `--jobs` concurrency.
    Pull(BundlePullArgs),
}

/// `decdn bundle pull` — fetch a bundle's blobs into a directory.
///
/// The bundle source is exactly one of `-i <file>` (a local manifest) or
/// `--hash <b3>` (fetch the manifest blob first, then its entries). Both then
/// run the same per-entry fetch loop. The network/chain/target flags come from
/// the flattened [`ClientFetchArgs`] — `--node-id` pins every entry to one node,
/// `--channel-id` (#1481) adopts one publisher-opened channel and pins the whole
/// bundle to its provider, otherwise each entry is discovered independently
/// (#936/#391). `--namespace` (ADR 002) routes every cache-miss origin pull to a
/// namespace's authorized origins.
#[derive(Args, Debug)]
#[command(group(ArgGroup::new("bundle_source").required(true).args(["input", "hash"])))]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent CLI flags (--overwrite/--dry-run/--json/--select), not a state machine"
)]
pub struct BundlePullArgs {
    /// Local bundle manifest file to pull. Mutually exclusive with `--hash`.
    #[arg(short = 'i', long, value_name = "FILE")]
    pub input: Option<PathBuf>,

    /// BLAKE3 hash (64 hex, optional `0x`/`b3:` prefix) of the bundle manifest
    /// blob to fetch first, then pull. Mutually exclusive with `-i`.
    #[arg(long, value_name = "HASH")]
    pub hash: Option<String>,

    /// Output directory the bundle's files are written under (created if
    /// absent). Each entry's relative path is resolved within this root;
    /// `..`/absolute/escaping paths are rejected.
    #[arg(short = 'o', long, value_name = "DIR")]
    pub output: PathBuf,

    /// Maximum concurrent entry fetches across the whole run. Each distinct blob
    /// is one unit of work holding one slot: a plain whole-file entry, or a
    /// range-dedup entry across its complement drive, donor splice, and any
    /// re-fetch. A byte range a sibling entry already holds is spliced from disk,
    /// not fetched, so it never takes a slot of its own.
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub jobs: usize,

    /// Maximum concurrent streams to one `(pool, signer, provider)` lane.
    /// Same-lane streams share one cumulative voucher watermark, and the serving
    /// node credits each stream's delivered bytes from that shared lane fairly
    /// (a slow stream is paid from the headroom a faster sibling opened), so
    /// same-lane concurrency is safe and this only bounds how many streams touch
    /// one provider at once. Cross-lane parallelism (distinct providers) is
    /// bounded by `--jobs` regardless.
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub max_lane_streams: usize,

    /// Give each entry whose fetch failed in a way another round can fix up to
    /// N more rounds after the first pass over the bundle. Each round probes the
    /// holders again, so a provider that failed earlier is a candidate again,
    /// and resumes from the entry's `.partial`, so no byte already paid for is
    /// paid again. The rounds back off (2 s, doubling, capped at 30 s). A
    /// failure every provider repeats (a rejected voucher, a blacklisted funder,
    /// an oversized blob, a spent pool, a manifest size no provider signs) and a
    /// local disk fault are never re-run. `0` turns the retry off.
    #[arg(long, value_name = "N", default_value_t = 2)]
    pub entry_retries: u32,

    /// Only pull entries whose POSIX relative path matches one of these globs
    /// (`models/*.bin`), matched against the manifest's `path` field, never the
    /// on-disk absolute path. Repeatable — an entry passes the include gate if it
    /// matches any one. Absent => every entry passes the gate. gitignore
    /// semantics: `*` does not cross `/`, `**` recurses.
    #[arg(long = "include", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Skip entries whose POSIX relative path matches one of these globs,
    /// matched against the manifest's `path` field. Repeatable — every pattern
    /// is OR-ed. `--exclude` wins over `--include`: an entry matching both is
    /// skipped. Same gitignore glob semantics as `--include`.
    #[arg(long = "exclude", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Choose which files to pull interactively: open the (post-`--include`/
    /// `--exclude`) file list in `$VISUAL`/`$EDITOR`, one `path` per line. Comment
    /// out (prefix `#`) or delete any line to skip that file; save and exit to pull
    /// the rest. Useful when you don't know a bundle's contents up front — the list
    /// is what the manifest actually holds. Requires an interactive terminal;
    /// incompatible with `--json` and `--dry-run`.
    #[arg(long)]
    pub select: bool,

    /// Re-fetch and overwrite entries whose destination file already exists.
    /// Default is skip-existing (resume-friendly): a path skips when its saved
    /// `.decdn-manifest.json` record still matches the file's hash, size, and
    /// mtime — the fast path, which trusts that gate without re-hashing — or,
    /// failing that, when re-hashing its on-disk bytes matches the new manifest.
    /// A file byte-identical to one already anywhere else in the output root is
    /// likewise reused with no download: it is linked (or copied) from that
    /// on-disk file once its hash is confirmed by a re-hash. Use `--overwrite`
    /// to force a full re-fetch of every entry, for example when the output
    /// directory may have been modified outside `decdn` (an in-place edit that
    /// keeps a file's size and mtime is otherwise trusted by the fast path). It
    /// skips consulting `.decdn-manifest.json` for skip decisions, so it
    /// disables both the skip-existing path and the whole-root reuse above; the
    /// file is still read and rewritten with the run's results.
    #[arg(long)]
    pub overwrite: bool,

    /// Print what would be fetched (paths + sizes) and exit without any network
    /// or chain activity.
    #[arg(long)]
    pub dry_run: bool,

    /// Emit a one-line JSON summary instead of human-readable output.
    #[arg(long)]
    pub json: bool,

    /// Namespace the whole bundle is published under (ADR 002 § Retrieval by
    /// namespace). Applies to EVERY fetch in the run — the manifest blob and each
    /// entry — so a serving node routes any cache-miss origin pull to that
    /// namespace's DAO-authorized origins. The manifest format carries no per-entry
    /// namespace, so this is necessarily bundle-level. Absent => no namespace:
    /// served best-effort from cache / DHT only.
    #[arg(long, value_name = "ID", value_parser = super::fetch::parse_fetch_namespace_id)]
    pub namespace: Option<u64>,

    /// Retrieval flags shared with `decdn fetch`: peer discovery, payment
    /// pool, and output handling.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
