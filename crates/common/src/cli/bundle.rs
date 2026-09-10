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

    /// Maximum concurrent blob fetches — whole-file entries and chunks of a
    /// chunked file alike — across the whole run. One many-chunk file can use the
    /// full budget by itself; a chunk shared between files is fetched once and does
    /// not consume an extra slot.
    #[arg(long, value_name = "N", default_value_t = 16)]
    pub jobs: usize,

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

    /// Re-fetch and overwrite entries whose destination file already exists.
    /// Default is skip-existing (resume-friendly: a completed file is only
    /// renamed into place after BLAKE3 verification, so a present file is good).
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
