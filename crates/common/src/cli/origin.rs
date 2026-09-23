//! Arguments for the `decdn origin` command group (issue #1904).
//!
//! `origin import` seeds a cache **origin store** with local content: for each
//! blob it writes the sharded data object `{hex[0..2]}/{hex}` and its sibling
//! pre-order bao outboard `{hex}.obao4` (per ADR 037 §Origin-tier pull-through),
//! the exact layout the daemon reads back. It is offline, config-free local
//! work — no running daemon, no `node.toml`, no chain — so it lives in its own
//! group rather than under `decdn node` (whose every subcommand talks to a
//! running node over the loopback admin surface, `appendix-local-admin-http.md`).
//!
//! `--to` is a local filesystem directory: the store is written to disk and
//! nowhere else. An S3 origin needs no separate writer, because the fs and S3
//! object layouts are byte-identical — import to a directory and `aws s3 sync`
//! it up to the bucket.

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
    /// canonical bundle manifest emitted (with `--dry-run`, printed to stdout
    /// instead) and itself imported, so the whole tree is retrievable by one
    /// bundle hash.
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

    /// Directory to populate — a local filesystem origin store rooted here,
    /// created if absent. This is the only write backend; pass the path
    /// directly (`--to /var/lib/decdn/origin`), with no scheme prefix.
    ///
    /// To populate an S3 origin, import to a local directory and then
    /// `aws s3 sync <dir> s3://<bucket>/<prefix>`: the fs and S3 object layouts
    /// are byte-identical, so the sync needs no renaming. An `s3://` or
    /// `http(s)://` value is rejected with that recipe rather than treated as a
    /// path.
    ///
    /// Required unless `--dry-run`.
    ///
    /// A `PathBuf`, not a `String`, so a non-UTF-8 directory path is accepted
    /// on Unix like `--input` is. Scheme sniffing (`s3://` / `http(s)://` /
    /// legacy `fs:`) runs on the UTF-8 view; a non-UTF-8 path cannot be one of
    /// those, so it is unambiguously a filesystem directory.
    #[arg(long, value_name = "DIR")]
    pub to: Option<PathBuf>,

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

    /// Prefix every manifest entry's path with this folder, so a `bundle pull`
    /// materializes the whole bundle under one directory (`<out>/<subfolder>/…`)
    /// rather than spilling its files directly into the output root. Must be a
    /// relative POSIX path with `/` separators — `..`, absolute paths, root
    /// prefixes, and backslashes are rejected; nesting (`a/b`) is allowed.
    /// Applies to a single-file import too (the one entry becomes
    /// `<subfolder>/<name>`).
    #[arg(long, value_name = "DIR")]
    pub subfolder: Option<String>,

    /// Emit a one-line JSON status report instead of a human summary. Written to
    /// stdout normally; under `--dry-run` it goes to stderr, because stdout then
    /// carries the canonical manifest bytes. `origin` is the `--to` target, or
    /// the literal `(dry-run)` when a dry run runs without `--to`.
    /// Shape:
    /// `{"imported":<n>,"bytes":<total>,"origin":"<target>","files":{"<path>":"b3:<hex>",...},"bundle_hash":"b3:<hex>"|null,"moved":<bool>,"optimized":<bool>,"chunks_total":<n>,"skipped_symlinks":<n>}`.
    #[arg(long)]
    pub json: bool,

    /// Content-defined-chunk each file and record the chunk boundaries as
    /// `{hash,size}` hints in the manifest entry; each file is still stored
    /// as one whole-file blob. A pull uses the hints to splice ranges it
    /// already holds instead of re-fetching them. Off by default (no hints).
    #[arg(long)]
    pub optimize: bool,

    /// Target average chunk size (`--optimize` only). The primary dial. Must be
    /// a power of two, at most 4 MiB (fastcdc's ceiling). Defaults to 4 MiB when
    /// `--optimize` is set (applied in the import wiring, not by clap, so an
    /// explicit `--chunk-avg` without `--optimize` is a detectable error).
    #[arg(long, value_name = "SIZE", value_parser = super::parse_byte_size)]
    pub chunk_avg: Option<u64>,

    /// Minimum chunk size override (`--optimize` only). Defaults to `avg/4`.
    /// At least 1 MiB (the payment interval), at most 1 MiB (fastcdc's ceiling).
    #[arg(long, value_name = "SIZE", value_parser = super::parse_byte_size)]
    pub chunk_min: Option<u64>,

    /// Maximum chunk size override (`--optimize` only). Defaults to `2*avg`.
    /// At most 16 MiB (fastcdc's ceiling).
    #[arg(long, value_name = "SIZE", value_parser = super::parse_byte_size)]
    pub chunk_max: Option<u64>,

    /// Compute and emit the manifest without writing any blobs. Prints the
    /// canonical manifest bytes to stdout (status goes to stderr). Makes `--to`
    /// optional — this is the local "just make me a manifest" path.
    #[arg(long)]
    pub dry_run: bool,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrap {
        #[command(flatten)]
        args: OriginImportArgs,
    }

    #[test]
    fn chunk_avg_is_none_by_default() {
        // No clap default: `None` = unset, so `decdn origin import` can reject
        // `--chunk-avg` given without `--optimize`. The 4 MiB default is applied
        // downstream, not by clap.
        let w = Wrap::try_parse_from(["x", "-i", "d", "--to", "/o"]).unwrap();
        assert_eq!(w.args.chunk_avg, None);
        assert!(!w.args.optimize);
        assert!(!w.args.dry_run);
    }

    #[test]
    fn to_is_optional() {
        let w = Wrap::try_parse_from(["x", "-i", "d", "--dry-run"]).unwrap();
        assert!(w.args.to.is_none());
        assert!(w.args.dry_run);
    }

    #[test]
    fn subfolder_is_none_by_default() {
        let w = Wrap::try_parse_from(["x", "-i", "d", "--to", "/o"]).unwrap();
        assert_eq!(w.args.subfolder, None);
    }

    #[test]
    fn parses_subfolder() {
        let w = Wrap::try_parse_from(["x", "-i", "d", "--to", "/o", "--subfolder", "assets/v1"])
            .unwrap();
        assert_eq!(w.args.subfolder.as_deref(), Some("assets/v1"));
    }

    #[test]
    fn parses_optimize_and_sizes() {
        let w = Wrap::try_parse_from([
            "x",
            "-i",
            "d",
            "--to",
            "/o",
            "--optimize",
            "--chunk-avg",
            "2MiB",
            "--chunk-min",
            "1MiB",
            "--chunk-max",
            "4MiB",
        ])
        .unwrap();
        assert!(w.args.optimize);
        assert_eq!(w.args.chunk_avg, Some(2 * 1024 * 1024));
        assert_eq!(w.args.chunk_min, Some(1024 * 1024));
        assert_eq!(w.args.chunk_max, Some(4 * 1024 * 1024));
    }
}
