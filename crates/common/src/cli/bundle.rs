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
//! Only `bundle create` ships in this iteration. `bundle pull` is blocked
//! on a client-side fetch primitive that doesn't exist yet — leaving the
//! `BundleCommand` enum single-variant signals "planned" without
//! committing to a wire shape.

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// Top-level `decdn bundle` group.
#[derive(Args, Debug)]
pub struct BundleArgs {
    #[command(subcommand)]
    pub cmd: BundleCommand,
}

/// Subcommands under `decdn bundle`. `Pull` is deferred (see
/// [`appendix-bundles`](../../../adr/appendix-bundles.md) § Future work).
#[derive(Subcommand, Debug)]
pub enum BundleCommand {
    /// Walk a directory, BLAKE3-hash every regular file, and emit a
    /// canonical JSON manifest. Same input directory yields byte-
    /// identical output, so the manifest's own BLAKE3 is stable across
    /// runs — that is what makes single-hash bundle distribution work.
    Create(BundleCreateArgs),
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
