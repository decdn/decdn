//! Arguments for `decdn get <URI>` (#2449): one command for a blob, a whole
//! bundle, or one bundle entry, named by a `decdn:` URI.
//!
//! The URI stays a `String` here: `decdn-common` does not depend on
//! `decdn-client`, which owns the `DecdnUri` parser. The `cli` binary parses it
//! and routes the target to the blob fetch or the bundle pull.

use std::path::PathBuf;

use clap::Args;

use super::fetch::ClientFetchArgs;

/// `decdn get` — pull what a `decdn:` URI names.
///
/// - `decdn:<hash>`: the raw blob. `-o` is the destination file, or `-` for
///   stdout.
/// - `decdn:<hash>/`: every entry of the bundle whose manifest has this hash.
///   `-o` is the output directory.
/// - `decdn:<hash>/<path>`: that one bundle entry. `-o` is the output
///   directory, and the file lands at `<OUT>/<path>`.
///
/// A `?ns=<id>` in the URI is the namespace routing hint (ADR 002). The
/// bundle-only flags are an error on a blob URI, and the entry filters
/// (`--include`, `--exclude`, `--select`) are an error on an entry URI.
#[derive(Args, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent CLI flags (--overwrite/--dry-run/--json/--select), not a state machine"
)]
pub struct GetArgs {
    /// The `decdn:` URI to pull: `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(value_name = "URI")]
    pub uri: String,

    /// The destination: a file (or `-` for stdout) for a blob URI, the output
    /// directory for a bundle or entry URI.
    #[arg(short = 'o', long, value_name = "PATH")]
    pub output: PathBuf,

    /// Bundle only: maximum concurrent entry fetches across the run [default:
    /// 4]. See `decdn bundle pull --help`.
    #[arg(long, value_name = "N")]
    pub jobs: Option<usize>,

    /// Bundle only: maximum concurrent streams to one `(pool, signer,
    /// provider)` lane, 1 to 64 [default: 4]. See `decdn bundle pull --help`.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=64))]
    pub max_lane_streams: Option<u16>,

    /// Whole-bundle only: pull only entries whose manifest path matches one of
    /// these globs. Repeatable.
    #[arg(long = "include", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Whole-bundle only: skip entries whose manifest path matches one of these
    /// globs. Repeatable. Wins over `--include`.
    #[arg(long = "exclude", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Whole-bundle only: choose the files to pull in `$VISUAL`/`$EDITOR`.
    #[arg(long)]
    pub select: bool,

    /// Bundle only: re-fetch and overwrite entries already on disk.
    #[arg(long)]
    pub overwrite: bool,

    /// Bundle only: print the entries the run would pull and exit, with no
    /// network or chain activity.
    #[arg(long)]
    pub dry_run: bool,

    /// Bundle only: emit a one-line JSON summary instead of human-readable
    /// output.
    #[arg(long)]
    pub json: bool,

    /// Retrieval flags shared with `decdn fetch` and `decdn bundle pull`: peer
    /// discovery, chain and payment-pool coordinates, the source cap and the
    /// give-up limit.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
