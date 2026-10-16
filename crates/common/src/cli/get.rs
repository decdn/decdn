//! Arguments for `decdn get <URI>` (#2449): one command for a blob, a whole
//! bundle, or one bundle entry, named by a `decdn:` URI
//! (`adr/appendix-bundles.md` § `decdn:` URIs).
//!
//! The URI stays a `String` here: `decdn-common` does not depend on
//! `decdn-client`, which owns the `DecdnUri` parser. The `cli` binary parses it
//! and routes the target to the blob fetch or the bundle pull.

use std::path::PathBuf;

use clap::Args;

use super::fetch::ClientFetchArgs;

/// The default `--jobs` of a bundle pull.
pub const DEFAULT_JOBS: usize = 4;

/// The default `--max-lane-streams` of a bundle pull.
pub const DEFAULT_MAX_LANE_STREAMS: u16 = 4;

/// `decdn get` — pull what a `decdn:` URI names over the paid `cdn/client/v1`
/// path.
///
/// - `decdn:<hash>`: the raw blob. `-o` is the destination file, or `-` for
///   stdout. Without `-o`, the blob lands at `./<hash>`.
/// - `decdn:<hash>/`: every entry of the bundle whose manifest has this hash.
///   `-o` is the output directory. Without `-o`, it is `.`.
/// - `decdn:<hash>/<path>`: that one bundle entry. `-o` is the output
///   directory, and the file lands at `<OUT>/<path>`. Without `-o`, the file
///   lands at `./<name>`, where `<name>` is the last segment of `<path>`.
///   With `-o -`, the entry streams to stdout as one blob and takes no
///   bundle-only flag.
///
/// A `?ns=<id>` in the URI is the namespace routing hint (ADR 002) for every
/// request of the run: the manifest blob and each entry. The bundle-only flags
/// are an error on a blob URI, and the entry filters (`--include`,
/// `--exclude`, `--select`) are an error on an entry URI.
#[derive(Args, Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent CLI flags (--overwrite/--dry-run/--json/--select), not a state machine"
)]
pub struct GetArgs {
    /// The `decdn:` URI to pull: `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(value_name = "URI")]
    pub uri: String,

    /// The destination. For a blob URI, the file to write (atomically: temp
    /// in the same directory, then rename), or `-` to stream verified bytes to
    /// stdout. For a bundle or entry URI, the output directory (created if
    /// absent); each entry's relative path resolves within it, and
    /// `..`/absolute/escaping paths are rejected. An entry URI also takes `-`,
    /// to stream that one entry to stdout. Without `-o`, a blob lands at
    /// `./<hash>`, a bundle in `.`, and an entry at `./<name>` (the last
    /// segment of its path); a bundle or entry pull then refuses a path that
    /// holds a file no earlier pull wrote, unless `--overwrite`.
    #[arg(short = 'o', long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Bundle only: maximum concurrent entry fetches across the whole run
    /// [default: 4]. Each distinct blob is one unit of work holding one slot:
    /// a plain whole-file entry, or a range-dedup entry across its complement
    /// drive, donor splice, and any re-fetch. A range-dedup entry that only
    /// waits for a sibling to finish the chunks it splices gives its slot to a
    /// queued entry until then. A byte range a sibling entry already holds is
    /// spliced from disk, not fetched, so it never takes a slot of its own.
    #[arg(long, value_name = "N")]
    pub jobs: Option<usize>,

    /// Bundle only: maximum concurrent streams to one `(pool, signer,
    /// provider)` lane [default: 4]. Same-lane streams share one cumulative
    /// voucher watermark, and the serving node credits each stream's delivered
    /// bytes from that shared lane fairly (a slow stream is paid from the
    /// headroom a faster sibling opened), so same-lane concurrency is safe and
    /// this only bounds how many streams touch one provider at once.
    /// Cross-lane parallelism (distinct providers) is bounded by `--jobs`
    /// regardless.
    ///
    /// One connection per node carries every stream of the command, so the
    /// bound (1 to 64) stays below the node's limit of concurrent streams on
    /// one connection.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=64))]
    pub max_lane_streams: Option<u16>,

    /// Whole-bundle only: pull only entries whose POSIX relative path matches
    /// one of these globs (`models/*.bin`), matched against the manifest's
    /// `path` field, never the on-disk absolute path. Repeatable: an entry
    /// passes the include gate if it matches any one. Absent => every entry
    /// passes the gate. gitignore semantics: `*` does not cross `/`, `**`
    /// recurses.
    #[arg(long = "include", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Whole-bundle only: skip entries whose POSIX relative path matches one
    /// of these globs, matched against the manifest's `path` field.
    /// Repeatable: every pattern is OR-ed. `--exclude` wins over `--include`:
    /// an entry matching both is skipped. Same gitignore glob semantics as
    /// `--include`.
    #[arg(long = "exclude", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Whole-bundle only: choose which files to pull interactively. Opens the
    /// (post-`--include`/`--exclude`) file list in `$VISUAL`/`$EDITOR`, one
    /// `path` per line. Comment out (prefix `#`) or delete any line to skip
    /// that file; save and exit to pull the rest. Requires an interactive
    /// terminal; incompatible with `--json` and `--dry-run`.
    #[arg(long)]
    pub select: bool,

    /// Bundle only: re-fetch and overwrite entries whose destination file
    /// already exists. Default is skip-existing (resume-friendly): a path
    /// skips when its saved `.decdn-manifest.json` record still matches the
    /// file's hash, size, and mtime — the fast path, which trusts that gate
    /// without re-hashing — or, failing that, when re-hashing its on-disk
    /// bytes matches the manifest. A file byte-identical to one already
    /// anywhere else in the output root is likewise reused with no download:
    /// it is linked (or copied) from that on-disk file once its hash is
    /// confirmed by a re-hash. Use `--overwrite` to force a full re-fetch of
    /// every entry, for example when the output directory may have been
    /// modified outside `decdn` (an in-place edit that keeps a file's size and
    /// mtime is otherwise trusted by the fast path). It skips consulting
    /// `.decdn-manifest.json` for skip decisions, so it disables both the
    /// skip-existing path and the whole-root reuse above; the file is still
    /// read and rewritten with the run's results. It also fetches the
    /// manifest again instead of reading the output root's cached copy.
    #[arg(long)]
    pub overwrite: bool,

    /// Bundle only: print the entries the run would pull (paths + sizes, after
    /// the entry filters; files already present are still listed) and exit
    /// without any network or chain activity. Entries are listed only when an
    /// earlier pull into the same output directory cached the bundle manifest.
    #[arg(long)]
    pub dry_run: bool,

    /// Bundle only: emit a one-line JSON summary instead of human-readable
    /// output.
    #[arg(long)]
    pub json: bool,

    /// Retrieval flags: peer discovery, chain and payment-pool coordinates,
    /// the source cap and the give-up limit.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
