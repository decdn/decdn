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
//! `bundle create` produces manifests; `bundle pull` (#391) realizes them —
//! fetching every referenced blob over the paid `cdn/client/v1` path
//! (`decdn fetch`'s primitive), with per-entry node discovery and bounded
//! concurrency.

use std::path::PathBuf;

use clap::{ArgGroup, Args, Subcommand};

use super::fetch::ClientFetchArgs;

/// Top-level `decdn bundle` group.
#[derive(Args, Debug)]
pub struct BundleArgs {
    #[command(subcommand)]
    pub cmd: BundleCommand,
}

/// Subcommands under `decdn bundle`.
// `Pull` is larger than `Create` (it flattens the full `ClientFetchArgs`), but
// this enum is parsed exactly once per process; boxing the variant only to
// satisfy the lint would fight clap's `Subcommand` derive for no real saving.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
pub enum BundleCommand {
    /// Walk a directory, BLAKE3-hash every regular file, and emit a
    /// canonical JSON manifest. Same input directory yields byte-
    /// identical output, so the manifest's own BLAKE3 is stable across
    /// runs — that is what makes single-hash bundle distribution work.
    Create(BundleCreateArgs),

    /// Fetch every blob a bundle references into an output directory over the
    /// paid `cdn/client/v1` path (#391). The bundle is read from a local file
    /// (`-i`) or fetched first by its own hash (`--hash`); each entry is then
    /// discovered + fetched independently, with `--jobs` concurrency.
    Pull(BundlePullArgs),
}

/// `decdn bundle create` — produce a bundle manifest from a directory.
#[derive(Args, Debug)]
pub struct BundleCreateArgs {
    /// Directory to bundle. Walked recursively. Symlinks are skipped
    /// unless `--follow-symlinks` is passed; symlinks that resolve
    /// outside the input root are always rejected, even when following
    /// is enabled, to keep the bundle's path field honest.
    #[arg(short = 'i', long, value_name = "DIR")]
    pub input: PathBuf,

    /// Output bundle JSON file. Overwritten if it exists. The file is
    /// always single-line compact JSON with no trailing newline — see
    /// `appendix-bundles.md` § Determinism.
    #[arg(short = 'o', long, value_name = "FILE")]
    pub output: PathBuf,

    /// Follow symlinks during the walk. Off by default. Even when on,
    /// symlinks resolving outside the input root are a hard error: the
    /// bundle's relative-path field cannot describe an external target.
    #[arg(long)]
    pub follow_symlinks: bool,

    /// Glob pattern of paths to exclude, matched against the POSIX
    /// relative path (`a/b.txt`, never the absolute path). Repeatable
    /// — every pattern is OR-ed together.
    #[arg(long = "exclude", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Emit a one-line JSON status report to stdout instead of a human
    /// summary. The bundle file itself is always JSON; this flag only
    /// controls the operator-facing status. Shape:
    /// `{"bundle":"<path>","entries":<n>,"total_size":<bytes>,
    /// "bundle_hash":"b3:<hex>","skipped_symlinks":<n>}`. The
    /// `bundle_hash` is the BLAKE3 of the emitted bundle bytes — the
    /// hash publishers distribute.
    #[arg(long)]
    pub json: bool,
}

/// `decdn bundle pull` — fetch a bundle's blobs into a directory.
///
/// The bundle source is exactly one of `-i <file>` (a local manifest) or
/// `--hash <b3>` (fetch the manifest blob first, then its entries). Both then
/// run the same per-entry fetch loop. The network/chain/target flags come from
/// the flattened [`ClientFetchArgs`] — `--node-id` pins every entry to one node,
/// otherwise each entry is discovered independently (#936/#391).
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

    /// Maximum entries fetched concurrently. Fetches sharing one provider's
    /// channel are still serialized (a channel's vouchers use a strictly
    /// increasing nonce), so effective parallelism is bounded by the number of
    /// distinct providers in flight.
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub jobs: usize,

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
    /// namespace). Applies to EVERY fetch in the run — the manifest blob, each
    /// `DECDNMAN` chunk, and each entry — so a serving node routes any cache-miss
    /// origin pull to that namespace's DAO-authorized origins. The manifest format
    /// carries no per-entry namespace, so this is necessarily bundle-level. Absent
    /// => no namespace: served best-effort from cache / DHT only.
    #[arg(long, value_name = "ID", value_parser = super::fetch::parse_fetch_namespace_id)]
    pub namespace: Option<u64>,

    #[command(flatten)]
    pub common: ClientFetchArgs,
}
