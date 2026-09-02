//! Arguments for the `decdn origin` command group (issue #1904).
//!
//! `origin import` seeds a cache **origin store** with local content: for each
//! blob it writes the sharded data object `{hex[0..2]}/{hex}` and its sibling
//! pre-order bao outboard `{hex}.obao4` (per ADR 037 §Origin-tier pull-through),
//! the exact layout the daemon reads back. It is offline, config-free local
//! work — no running daemon, no `node.toml`, no chain — so it lives in its own
//! group rather than under `decdn node` (whose every subcommand talks to a
//! running node over the loopback admin surface, ADR 025).
//!
//! The hash + outboard computation is backend-independent (an origin store is
//! content-addressed with an identical object layout across the fs and s3
//! backends); only the final write differs. The `--to` target selects the
//! backend, so an `s3://` writer is an additive follow-up rather than a rewrite.
//! v1 implements the `fs:<dir>` target.

use std::path::PathBuf;

use clap::{Args, Subcommand};

/// Top-level `decdn origin` group.
#[derive(Args, Debug)]
pub struct OriginArgs {
    /// The `decdn origin` subcommand to run.
    #[command(subcommand)]
    pub cmd: OriginCommand,
}

/// Subcommands under `decdn origin`.
#[derive(Subcommand, Debug)]
pub enum OriginCommand {
    /// Import local content into a cache origin store, writing each blob's
    /// sharded data object and its `{hex}.obao4` outboard in the layout the
    /// daemon reads. A directory is walked, every regular file imported, and a
    /// canonical bundle manifest (byte-identical to `bundle create`) emitted and
    /// itself imported, so the whole tree is retrievable by one bundle hash.
    Import(OriginImportArgs),
}

/// `decdn origin import` — seed a cache origin store from a local file or tree.
// The flags (`--move`, `--force`, `--follow-symlinks`, `--json`) are independent
// operator toggles on one offline command, not a state machine — a bitflags or
// two-variant-enum refactor would only obscure the clap surface.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent CLI flags, not modelled state"
)]
#[derive(Args, Debug)]
pub struct OriginImportArgs {
    /// Local file or directory to import. A directory is walked recursively
    /// (see `--follow-symlinks` / `--exclude`); every regular file becomes a
    /// content-addressed blob in the target.
    #[arg(short = 'i', long, value_name = "PATH")]
    pub input: PathBuf,

    /// Target origin store to populate, as `<backend>:<location>`:
    ///
    /// - `fs:<dir>` — a local filesystem origin store rooted at `<dir>`
    ///   (created if absent). **Implemented.**
    /// - `s3://<bucket>/<prefix>` — an S3 origin store. **Not yet implemented**
    ///   (a follow-up; the object layout is identical, only the writer differs).
    ///
    /// An HTTP origin is a read-only static server, not a write target: seed the
    /// `fs:` layout onto the disk it serves instead.
    #[arg(long, value_name = "TARGET")]
    pub to: String,

    /// Move each source file into the target instead of copying it. Across
    /// filesystems this falls back to copy-then-unlink. A source whose content
    /// is already present in the target is removed too (the blob is there).
    #[arg(long = "move")]
    pub move_source: bool,

    /// When importing a directory, also write the canonical bundle manifest to
    /// this file (in addition to importing the manifest blob into the target).
    /// Ignored for a single-file import.
    #[arg(long, value_name = "FILE")]
    pub bundle: Option<PathBuf>,

    /// Overwrite a target object even when one already exists at its hash. By
    /// default an existing object is left in place (content-addressed: the same
    /// hash is the same bytes); a size mismatch is refused unless `--force`.
    #[arg(long)]
    pub force: bool,

    /// Follow symlinks during a directory walk. Off by default. Even when on, a
    /// symlink resolving outside the input root is a hard error: a manifest's
    /// relative-path field cannot describe an external target.
    #[arg(long)]
    pub follow_symlinks: bool,

    /// Glob pattern of paths to exclude during a directory walk, matched against
    /// the POSIX relative path (`a/b.txt`, never the absolute path). Repeatable
    /// — every pattern is OR-ed together.
    #[arg(long = "exclude", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Emit a one-line JSON status report to stdout instead of a human summary.
    /// Shape:
    /// `{"imported":<n>,"bytes":<total>,"origin":"<target>","bundle_hash":"b3:<hex>"|null,"moved":<bool>}`.
    #[arg(long)]
    pub json: bool,
}
