//! The bundle route of `decdn get` (`decdn:<hash>/` and
//! `decdn:<hash>/<path>`): fetch every blob a bundle manifest references, or
//! the one entry the URI names, into an output directory over the paid
//! `cdn/client/v1` path (issues #391, #2449).
//!
//! The pull itself is [`decdn_client::bundle::BundlePuller`]; this module is
//! its command line. It resolves the flags and config into the puller's
//! inputs, fetches the manifest by its own BLAKE3 hash, keeps the URI's entry
//! or applies the `--include`/`--exclude` filter and the `--select` editor,
//! renders progress, maps Ctrl-C to the puller's cancel token, and prints the
//! summary (or the `--json` report).

use std::collections::HashSet;
use std::io::IsTerminal as _;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context as _, anyhow, bail};
use decdn_client::PoolReplaced;
use decdn_client::bundle::{
    BundleEnd, BundleManifest, BundleObserver, BundlePuller, BundleReport, EntryLanded,
    FaultContext, FaultHints, FileObserver, ForeignReason, Funding, ManifestEntry, NotAManifest,
    STAGING_DIR, select_nodes,
};
use decdn_client::buyer_pool::UnconfirmedEscrows;
use decdn_client::endpoint as client_endpoint;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::GetArgs;
use decdn_common::cli::get::{DEFAULT_JOBS, DEFAULT_MAX_LANE_STREAMS};
use decdn_common::config::load_file_config;
use decdn_incentive::CapabilityGrant;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::eth_identity::{PasswordUse, load_signer};
use iroh::{Endpoint, RelayUrl};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::buyer_store::open_client_store_for_buy;
use super::fetch;
use super::get::Destination;
use super::interrupt::{Interrupt, Interrupted, name_unconfirmed_escrows};
use super::manifest::build_glob_set;
use super::pull_progress::{self, PullProgress};

/// A manifest cut down to the entries this run pulls, and how many entries
/// the `--include`/`--exclude` filter and the `--select` editor dropped.
#[derive(Debug)]
struct Kept {
    manifest: BundleManifest,
    /// Entries the run does not pull: filtered out or deselected.
    excluded: u64,
}

/// The run label for the progress header: the output directory's final component,
/// or its whole path when it has none (a bare root).
fn run_label(output: &Path) -> String {
    output.file_name().map_or_else(
        || output.to_string_lossy().into_owned(),
        |n| n.to_string_lossy().into_owned(),
    )
}

/// What selects the manifest entries a pull run fetches: the one entry an
/// entry URI names, or the compiled `--include`/`--exclude` globs.
///
/// An entry URI (`decdn:<hash>/<path>`) keeps exactly the entry whose manifest
/// `path` equals `<path>`, byte for byte, with no glob semantics.
///
/// The globs match an entry's POSIX relative `path` — the manifest field
/// (`models/a.bin`), never the on-disk absolute path — with the same gitignore
/// glob dialect `origin import --exclude` uses (`*` does not cross `/`, `**`
/// recurses), via [`build_glob_set`]. An entry is kept iff it passes the
/// include gate AND matches no exclude. The include gate is open when no
/// `--include` was given (every entry passes) and otherwise requires a match
/// against at least one include pattern; `--exclude` always wins over
/// `--include`.
///
/// Every pattern matches the whole path from the bundle root, so `metal/*`
/// matches `metal/model.bin` but not `gpt/metal/model.bin`; `**/metal/*`
/// matches both. A pattern that matches no entry is almost always this mistake,
/// so [`Self::apply_and_warn`] names each one.
#[derive(Debug)]
struct EntryFilter {
    /// The manifest path an entry URI names.
    entry: Option<String>,
    include: globset::GlobSet,
    /// The `--include` patterns as given, in the order [`build_glob_set`]
    /// compiled them, so a [`globset::GlobSet::matches`] index names its
    /// pattern.
    include_patterns: Vec<String>,
    exclude: globset::GlobSet,
    /// The `--exclude` patterns as given, indexed like `include_patterns`.
    exclude_patterns: Vec<String>,
    /// The file name the entry an entry URI names lands at, directly in the
    /// output root, in place of its manifest `path`. Set only with `entry`.
    land_as: Option<String>,
}

/// A `--include` or `--exclude` pattern that matched no entry of the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Unmatched {
    /// The flag the pattern came from: `--include` or `--exclude`.
    flag: &'static str,
    /// The pattern as given.
    pattern: String,
    /// The same pattern under a leading `**/`, when that form matches at least
    /// one entry: the likely intent of a pattern written as if it matched at
    /// any depth.
    suggestion: Option<String>,
}

impl Unmatched {
    /// The warning line for this pattern, without the `warning: ` prefix.
    fn warning(&self) -> String {
        let hint = self
            .suggestion
            .as_ref()
            .map_or_else(String::new, |suggestion| {
                format!(
                    " (a pattern matches the whole path from the bundle root; did you mean \
                     '{suggestion}'?)"
                )
            });
        format!("{} '{}' matched no entries{hint}", self.flag, self.pattern)
    }
}

/// What [`EntryFilter::apply_reporting`] hands back.
#[derive(Debug)]
struct FilterPass {
    /// The entries the filter keeps, in manifest order.
    kept: Vec<ManifestEntry>,
    /// How many entries the filter dropped.
    excluded: u64,
    /// Each pattern that matched no entry, `--include` ones first.
    unmatched: Vec<Unmatched>,
}

impl EntryFilter {
    /// Compile the run's entry selection: the URI's `entry` path, and the
    /// `--include`/`--exclude` patterns. A malformed glob is a hard error naming
    /// the flag it came from.
    fn compile(
        entry: Option<String>,
        include: &[String],
        exclude: &[String],
    ) -> anyhow::Result<Self> {
        Ok(Self {
            entry,
            include: build_glob_set(include, "--include")?,
            include_patterns: include.to_vec(),
            exclude: build_glob_set(exclude, "--exclude")?,
            exclude_patterns: exclude.to_vec(),
            land_as: None,
        })
    }

    /// This filter with the entry the URI names landing at `name`, directly in
    /// the output root. A filter with no entry URI has no entry to rename, so
    /// it drops `name`.
    fn landing_as(mut self, name: Option<String>) -> Self {
        self.land_as = name.filter(|_| self.entry.is_some());
        self
    }

    /// Retain only the entries the filter keeps, preserving manifest order, and
    /// report the patterns that matched no entry. A pattern's hits are counted
    /// over every entry of `entries`, before either gate drops any, so an
    /// `--exclude` that matches only entries the include gate already dropped
    /// still counts as a match.
    fn apply_reporting(&self, entries: Vec<ManifestEntry>) -> FilterPass {
        let mut include_hits = vec![false; self.include_patterns.len()];
        let mut exclude_hits = vec![false; self.exclude_patterns.len()];
        let keep: Vec<bool> = entries
            .iter()
            .map(|e| {
                let p = Path::new(&e.path);
                let included = mark_hits(&self.include, p, &mut include_hits);
                let excluded = mark_hits(&self.exclude, p, &mut exclude_hits);
                let named = self.entry.as_ref().is_none_or(|entry| *entry == e.path);
                named && (self.include_patterns.is_empty() || included) && !excluded
            })
            .collect();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        let unmatched = unmatched_patterns("--include", &self.include_patterns, &include_hits)
            .chain(unmatched_patterns(
                "--exclude",
                &self.exclude_patterns,
                &exclude_hits,
            ))
            .map(|mut u| {
                u.suggestion = anywhere_suggestion(&u.pattern, &paths);
                u
            })
            .collect();
        let raw = entries.len();
        let kept: Vec<ManifestEntry> = entries
            .into_iter()
            .zip(keep)
            .filter_map(|(e, keep)| keep.then_some(e))
            .collect();
        let excluded = u64::try_from(raw.saturating_sub(kept.len())).unwrap_or(u64::MAX);
        FilterPass {
            kept,
            excluded,
            unmatched,
        }
    }

    /// [`Self::apply_reporting`], with one `warning:` line on stderr for each
    /// pattern that matched no entry. Returns the kept entries, each moved to
    /// `land_as` with [`ManifestEntry::land_at`] when it is set, and how many
    /// the filter dropped.
    ///
    /// # Errors
    ///
    /// When an entry URI names a path the manifest does not hold.
    fn apply_and_warn(
        &self,
        entries: Vec<ManifestEntry>,
    ) -> anyhow::Result<(Vec<ManifestEntry>, u64)> {
        let pass = self.apply_reporting(entries);
        for u in &pass.unmatched {
            eprintln!("warning: {}", u.warning());
        }
        if let Some(entry) = &self.entry {
            anyhow::ensure!(
                !pass.kept.is_empty(),
                "the bundle has no entry {entry:?}; a URI path names one manifest `path` exactly"
            );
        }
        let kept = match &self.land_as {
            Some(name) => pass.kept.into_iter().map(|e| e.land_at(name)).collect(),
            None => pass.kept,
        };
        Ok((kept, pass.excluded))
    }
}

/// Refuse a pull into a default destination (no `-o`) that may replace a file
/// no earlier pull of the same entry left there unchanged: one no pull wrote,
/// one that changed after a pull wrote it, or one a pull wrote from another
/// bundle entry. A pull fetches over a file whose bytes differ from its
/// entry's, and the user did not choose this directory. `--overwrite` asks for
/// the replacement, so it passes.
///
/// # Errors
///
/// When an entry path of `manifest` under `out` holds a file, directory or
/// link that [`decdn_client::bundle::foreign_paths`] names.
fn refuse_foreign(args: &GetArgs, out: &Path, manifest: &BundleManifest) -> anyhow::Result<()> {
    if args.output.is_some() || args.overwrite {
        return Ok(());
    }
    let found = decdn_client::bundle::foreign_paths(manifest, out);
    let Some(first) = found.first() else {
        return Ok(());
    };
    let more = match found.len() {
        1 => String::new(),
        n => format!(" (and {} more)", n.saturating_sub(1)),
    };
    let from = |uri: &Option<DecdnUri>| {
        uri.as_ref().map_or_else(
            || "a source this build cannot read".to_owned(),
            ToString::to_string,
        )
    };
    let why = match &first.reason {
        ForeignReason::Unrecorded => "no earlier `decdn get` wrote it".to_owned(),
        ForeignReason::UnreadableCache(cause) => {
            format!("the skip-cache cannot say which `decdn get` wrote it ({cause})")
        }
        ForeignReason::Changed(uri) => format!(
            "it changed after an earlier `decdn get` wrote it from {}",
            from(uri)
        ),
        ForeignReason::OtherSource(uri) => {
            format!("an earlier `decdn get` wrote it from {}", from(uri))
        }
        _ => "no earlier `decdn get` of this entry wrote it".to_owned(),
    };
    anyhow::bail!(
        "refusing to replace {}{more}: {why}; pass `-o <dir>` to pull into another \
         directory, or `-o .` to pull here and replace each such file whose bytes differ",
        out.join(&first.path).display()
    )
}

/// Mark in `hits` every pattern of `set` that matches `path`, and return
/// whether any did.
fn mark_hits(set: &globset::GlobSet, path: &Path, hits: &mut [bool]) -> bool {
    let matched = set.matches(path);
    for i in &matched {
        if let Some(hit) = hits.get_mut(*i) {
            *hit = true;
        }
    }
    !matched.is_empty()
}

/// The patterns of `flag` whose `hits` slot is unset, with no suggestion yet.
fn unmatched_patterns<'a>(
    flag: &'static str,
    patterns: &'a [String],
    hits: &'a [bool],
) -> impl Iterator<Item = Unmatched> + 'a {
    patterns
        .iter()
        .zip(hits)
        .filter(|(_, hit)| !**hit)
        .map(move |(pattern, _)| Unmatched {
            flag,
            pattern: pattern.clone(),
            suggestion: None,
        })
}

/// `**/<pattern>`, when `pattern` does not start with `**` or `/` and that form
/// matches at least one of `paths`.
fn anywhere_suggestion(pattern: &str, paths: &[&str]) -> Option<String> {
    if pattern.starts_with("**") || pattern.starts_with('/') {
        return None;
    }
    let anywhere = format!("**/{pattern}");
    let set = build_glob_set(std::slice::from_ref(&anywhere), "suggestion").ok()?;
    paths
        .iter()
        .any(|p| set.is_match(Path::new(p)))
        .then_some(anywhere)
}

/// Header written above the file list in the `--select` editor buffer. Explains
/// the git-rebase-style convention: commented or deleted lines are skipped.
const SELECT_HEADER: &str = "\
# decdn get --select — choose which files to pull.
#
# Comment out (prefix with #) or delete any line to SKIP that file.
# Save and exit to pull every file still listed below; delete them all to pull
# nothing. Reordering has no effect, and you cannot add files here.
#
# Each line is  <path>\t<size>  — only the path (before the tab) is read.
";

/// Render the `--select` editor buffer: the header, then one line per entry as
/// `<path>\t<human size>`. The size is a right-hand annotation for context only;
/// [`parse_selection`] reads just the path before the first tab.
fn render_selection(entries: &[ManifestEntry]) -> String {
    let mut out = String::from(SELECT_HEADER);
    for e in entries {
        let size = e.size.map_or_else(|| "?".to_string(), human_bytes);
        out.push_str(&e.path);
        out.push('\t');
        out.push_str(&size);
        out.push('\n');
    }
    out
}

/// Reject a bundle whose paths cannot round-trip through the `--select` editor
/// buffer exactly. [`parse_selection`] reads a kept line as the text before the
/// first tab, trimmed, and skips a line whose first non-whitespace character is
/// `#`. A path therefore fails to round-trip when it:
/// - differs from its trimmed form (leading/trailing whitespace, which
///   [`parse_selection`] strips — including whitespace before a `#`, which would
///   make the line read as a comment and silently drop the file), or
/// - has `#` as its first non-whitespace character (read as a comment), or
/// - contains a tab, newline, or carriage return (splits or breaks its line).
///
/// Such paths are pathological for a POSIX relative filename; `--select` refuses
/// them loudly rather than silently dropping a file the user meant to keep.
/// `--include`/`--exclude` still handle these bundles.
fn check_selectable(entries: &[ManifestEntry]) -> anyhow::Result<()> {
    if let Some(bad) = entries.iter().find(|e| {
        let p = e.path.as_str();
        p != p.trim() || p.trim_start().starts_with('#') || p.contains(['\t', '\n', '\r'])
    }) {
        bail!(
            "--select cannot represent the path {:?} (paths with leading/trailing \
             whitespace, a leading '#', or a tab/newline are unsupported); use \
             --include/--exclude instead",
            bad.path
        );
    }
    Ok(())
}

/// Apply the edited `--select` buffer back onto the entry list: keep only entries
/// whose path still appears on a non-comment, non-blank line, preserving the
/// original manifest order. A kept line whose path matches no bundle entry is a
/// hard error — the editor only removes files, so an unmatched line is a typo,
/// not an addition, and silently dropping it would be worse than failing.
fn parse_selection(
    edited: &str,
    entries: Vec<ManifestEntry>,
) -> anyhow::Result<Vec<ManifestEntry>> {
    let known: HashSet<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    let mut kept: HashSet<String> = HashSet::new();
    let mut unknown: Vec<&str> = Vec::new();
    for line in edited.lines() {
        if line.trim_start().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        // The path is the text before the size annotation (first tab), trimmed.
        let path = line.split_once('\t').map_or(line, |(p, _)| p).trim();
        if path.is_empty() {
            continue;
        }
        if known.contains(path) {
            kept.insert(path.to_string());
        } else {
            unknown.push(path);
        }
    }
    if !unknown.is_empty() {
        bail!(
            "--select: these lines match no bundle entry (editing only removes files, \
             it cannot add them): {}",
            unknown.join(", ")
        );
    }
    Ok(entries
        .into_iter()
        .filter(|e| kept.contains(&e.path))
        .collect())
}

/// Apply the interactive `--select` step to a finalized manifest, or pass it
/// through unchanged when `--select` is off. Each deselected entry adds to the
/// excluded count. `Ok(None)` means the user deselected every file:
/// [`report_nothing_to_fetch`] was already called, so the run is done.
fn maybe_select(
    args: &GetArgs,
    out: &Path,
    uri: &DecdnUri,
    mut kept: Kept,
) -> anyhow::Result<Option<Kept>> {
    if args.select {
        kept = apply_selection(kept, select_entries)?;
        if kept.manifest.entries.is_empty() {
            report_nothing_to_fetch(NothingReason::Deselected);
            warn_leftover_partials(out, &[], uri.hash);
            return Ok(None);
        }
    }
    Ok(Some(kept))
}

/// Replace `kept`'s entries with the ones `select` keeps, and add each entry it
/// drops to the excluded count.
fn apply_selection(
    mut kept: Kept,
    select: impl FnOnce(Vec<ManifestEntry>) -> anyhow::Result<Vec<ManifestEntry>>,
) -> anyhow::Result<Kept> {
    let offered = kept.manifest.entries.len();
    kept.manifest.entries = select(kept.manifest.entries)?;
    let dropped = offered.saturating_sub(kept.manifest.entries.len());
    kept.excluded = kept
        .excluded
        .saturating_add(u64::try_from(dropped).unwrap_or(u64::MAX));
    Ok(kept)
}

/// Reject a `--select` invocation that cannot work: it opens an editor, so it
/// needs an interactive terminal (both stdin and stdout) and is incompatible with
/// the non-interactive `--json` and `--dry-run` modes. A no-op when `--select` is
/// not set.
fn check_select_flags(args: &GetArgs) -> anyhow::Result<()> {
    if !args.select {
        return Ok(());
    }
    if args.json {
        bail!("--select cannot be combined with --json (it needs an interactive editor)");
    }
    if args.dry_run {
        bail!("--select cannot be combined with --dry-run (it needs an interactive editor)");
    }
    if !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        bail!("--select needs an interactive terminal; use --include/--exclude instead");
    }
    Ok(())
}

/// Run the interactive `--select` step: render the entry list, open it in the
/// user's editor, and apply the edited buffer back. All decision logic lives in
/// the tested [`render_selection`]/[`parse_selection`]/[`check_selectable`]; this
/// only wires them to the editor. The caller has already verified an interactive
/// terminal and that `--json`/`--dry-run` are not set.
fn select_entries(entries: Vec<ManifestEntry>) -> anyhow::Result<Vec<ManifestEntry>> {
    check_selectable(&entries)?;
    let edited = edit_in_editor(&render_selection(&entries))?;
    parse_selection(&edited, entries)
}

/// Resolve the editor command from `$VISUAL`/`$EDITOR` into program + arguments,
/// falling back to `vi`. `$VISUAL` wins over `$EDITOR`; a missing or
/// blank/whitespace-only value is treated as unset. The value may carry arguments
/// (`code --wait`): it is split on ASCII whitespace, with no shell quoting or
/// escaping — an argument cannot itself contain a space. The scratch file path is
/// appended by the caller, not here. Never empty — the fallback guarantees at
/// least `["vi"]`.
fn editor_command(visual: Option<&str>, editor: Option<&str>) -> Vec<String> {
    let chosen = [visual, editor]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .unwrap_or("vi");
    chosen.split_whitespace().map(String::from).collect()
}

/// Open `buffer` in the user's editor and return the saved contents. The editor
/// is `$VISUAL`, then `$EDITOR`, then `vi`; the value may carry arguments
/// (`code --wait`), split on ASCII whitespace with no shell quoting (see
/// [`editor_command`]). The buffer is staged in a temporary file that is removed
/// when this returns.
fn edit_in_editor(buffer: &str) -> anyhow::Result<String> {
    let mut file = tempfile::Builder::new()
        .prefix("decdn-select-")
        .suffix(".txt")
        .tempfile()
        .context("create --select scratch file")?;
    std::io::Write::write_all(file.as_file_mut(), buffer.as_bytes())
        .context("write --select scratch file")?;
    file.as_file()
        .sync_all()
        .context("sync --select scratch file")?;

    let command = editor_command(
        std::env::var("VISUAL").ok().as_deref(),
        std::env::var("EDITOR").ok().as_deref(),
    );
    let (program, rest) = command
        .split_first()
        .ok_or_else(|| anyhow!("empty editor command"))?;

    let status = std::process::Command::new(program)
        .args(rest)
        .arg(file.path())
        .status()
        .with_context(|| format!("launch editor {program:?}"))?;
    if !status.success() {
        bail!("editor {program:?} exited with {status}; aborting --select");
    }

    std::fs::read_to_string(file.path()).context("read back --select scratch file")
}

/// One-line `--json` summary. `fetched`/`linked`/`skipped`/`failed` are entry
/// counts; `downloaded` is the content bytes this run fetched and paid for across
/// the distinct blobs fetched (a blob shared across several paths counts once,
/// #1306; a range-dedup blob counts only the bytes it did not splice from disk)
/// and `reconstructed` is the total bytes written to disk this run — they diverge
/// when one blob is materialized to several paths or when range-dedup spliced part
/// of a blob. `downloaded` is a content-size tally, not an exact on-wire
/// measurement: it excludes bao proof overhead.
///
/// `paid_bytes` is the bytes this run's vouchers and hash-chain reveals paid
/// for: how far the run grew the claims the nodes can redeem, summed over every
/// lane. It is counted from payment, not from sizes, so it is wire bytes and
/// includes the bao proof overhead (a clean pull pays a little more than
/// `downloaded`). Content delivered twice, or paid for and then dropped after
/// a lane fault, counts each time it is paid. An entry that fails or is
/// interrupted counts here but not in `downloaded`; a later run resumes its
/// `.partial` and reports it as `resumed_bytes`.
///
/// `resumed_bytes` is the content bytes of a `.partial` prefix that an earlier,
/// interrupted run fetched and paid for, which this run resumed rather than
/// fetched again. It is never part of `downloaded`. A byte that this run also
/// spliced from disk counts only in `spliced_bytes`.
///
/// `excluded` counts the manifest entries this run does not pull: the ones the
/// `--include`/`--exclude` filter dropped and the ones deselected under
/// `--select`.
///
/// `reused` and `reused_bytes` report the whole-file dedup outcome, counted per
/// distinct blob exactly as `fetched`/`downloaded` are (a blob reused at several
/// paths counts once here; its extra destinations are `linked`): `reused` is the
/// count of distinct blobs materialized from an on-disk whole-file donor (verified
/// by re-hash before use), and `reused_bytes` sums those blobs' sizes once each —
/// bytes served from disk with no download and no payment.
///
/// `spliced_bytes` and `hints_ignored` report the range-dedup outcome so a run
/// whose hints saved bytes is distinguishable from one whose hints did not:
/// `spliced_bytes` is the total bytes served from a local donor splice (bytes NOT
/// downloaded or paid for), and `hints_ignored` counts range-dedup hints dropped
/// by a fault — a chunk set that failed to parse or whose sizes did not sum to the
/// file size, a donor that failed its verification re-hash (or was unreadable), or
/// a self-heal whole-file re-drive that discarded already-spliced donors. Both are
/// reporting only and never affect payment.
#[derive(Serialize)]
struct PullReport {
    output: String,
    fetched: u64,
    linked: u64,
    skipped: u64,
    /// Entries the filter or `--select` dropped.
    excluded: u64,
    failed: u64,
    downloaded: u64,
    /// Wire bytes this run's vouchers and hash-chain reveals paid for, over
    /// every lane.
    paid_bytes: u64,
    reconstructed: u64,
    spliced_bytes: u64,
    hints_ignored: u64,
    /// Content bytes of `.partial` prefixes that earlier runs fetched and this
    /// run resumed; never part of `downloaded`.
    resumed_bytes: u64,
    /// Count of distinct blobs materialized from an on-disk whole-file donor;
    /// extra destinations of the same blob are counted in `linked`.
    reused: u64,
    /// Sum of those blobs' sizes (once per blob) — bytes materialized from a
    /// whole-file donor with no download or payment.
    reused_bytes: u64,
}

/// Load the buyer's Ethereum signer (vouchers + any openPool/topUp tx),
/// prompting for the keystore password once. Password from
/// `$DECDN_KEYSTORE_PASSWORD`, else `--keystore-password-file`, else a TTY
/// prompt.
fn load_buyer_signer(chain: &fetch::ResolvedChain) -> anyhow::Result<Arc<PrivateKeySigner>> {
    let password = super::chain_ctx::read_keystore_password(
        &super::chain_ctx::password_sources(
            chain.keystore_password_file.as_deref(),
            PasswordUse::Unlock,
        ),
        "eth keystore password",
    )?
    .into_secret();
    Ok(Arc::new(load_signer(&chain.keystore, &password)?))
}

/// `decdn get decdn:<hash>/` and `decdn get decdn:<hash>/<path>`: pull the
/// bundle `uri` names, or its one entry, into `dest.path`. With
/// `dest.entry_name`, the entry lands at that name directly in `dest.path`.
pub(crate) async fn bundle_pull(
    args: &GetArgs,
    dest: &Destination,
    uri: &DecdnUri,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    // `-o -` is stdout, which a pull cannot write a tree to; the blob fetch
    // takes every `-o -` run. Fail rather than create a directory named `-`.
    anyhow::ensure!(
        dest.path != Path::new("-"),
        "`-o -` (stdout) does not take a bundle pull"
    );
    // Validate the flag combination first — BEFORE the dry-run short-circuit — so a
    // bad combo (e.g. `--provider-address` without `--node-id`) or an unusable
    // timeout pair is rejected even for `--dry-run`. This rule does not run at
    // parse time, so `bundle_pull` calls it BEFORE the dry-run short-circuit. It
    // is cheap and side-effect-free.
    args.common.validate()?;

    // Compile the entry selection BEFORE the dry-run short-circuit — same
    // rationale as `validate()`: a malformed glob is rejected before any
    // network, chain, or keystore work, and `--dry-run` reflects the filtered
    // plan too.
    let entry = match &uri.target {
        UriTarget::BundleEntry(path) => Some(path.clone()),
        _ => None,
    };
    let filter = EntryFilter::compile(entry, &args.include, &args.exclude)?
        .landing_as(dest.entry_name.clone());
    let out = dest.path.as_path();
    let filters_given = !args.include.is_empty() || !args.exclude.is_empty();

    // `--select` opens an editor: reject an incompatible combo or a non-terminal
    // up front — before any network, chain, or keystore work — so it fails fast
    // rather than after paying to fetch the manifest.
    check_select_flags(args)?;

    // Dry-run short-circuits before any network/chain/keystore activity.
    if args.dry_run {
        return dry_run(args, out, uri, &filter, filters_given);
    }

    let common = &args.common;
    let relays = client_endpoint::resolve_relays(common.relay_url.as_deref(), config_path)?;
    let disc = client_endpoint::client_discovery(config_path)?;
    let file = load_file_config(config_path)?;
    let mut chain = fetch::resolve_chain(common, &file)?;

    // Delegated adoption: `--capability`/`--capability-file` names a pool the
    // caller does NOT own and a capability authorizing this client's key to
    // spend against it (every entry pulls from that one pool). `None` => the
    // unchanged self-owned pool path; a delegated grant also zeroes the working
    // deposit in `chain`, so its funding recovery step has nothing to add.
    let grant = fetch::resolve_delegation_grant(common, &mut chain)?;

    // Same guard as the blob route, for the same reason and before the same
    // password prompt: a node's data dir is a buy target only when the
    // operator named it (#2082).
    let store: Arc<dyn BuyerPoolStore> = Arc::new(open_client_store_for_buy(
        &chain.site.data_dir,
        chain.data_dir_source,
        "pull",
    )?);
    let endpoint = client_endpoint::client_endpoint(&relays, &disc).await?;
    // The body runs in `pull_over`, so the endpoint closes on every exit —
    // success, an early return, or an error — and its open connections end
    // cleanly instead of being aborted on drop.
    let result = pull_over(
        args,
        out,
        uri,
        &chain,
        grant,
        &store,
        &endpoint,
        &relays,
        (&filter, filters_given),
    )
    .await;
    endpoint.close().await;
    result
}

/// The part of [`bundle_pull`] that runs over its open `endpoint`: resolve the
/// providers and the buyer signer, build the puller, obtain the manifest, and
/// pull it. `filter` is the compiled entry selection and whether `--include` or
/// `--exclude` was given.
#[allow(clippy::too_many_arguments)]
async fn pull_over(
    args: &GetArgs,
    out: &Path,
    uri: &DecdnUri,
    chain: &fetch::ResolvedChain,
    grant: Option<CapabilityGrant>,
    store: &Arc<dyn BuyerPoolStore>,
    endpoint: &Endpoint,
    relays: &[RelayUrl],
    (filter, filters_given): (&EntryFilter, bool),
) -> anyhow::Result<()> {
    let common = &args.common;
    // The node selection is resolved BEFORE the keystore prompt, so a failed
    // discovery never prompts. A registry read is bounded by `--timeout-ms`
    // (#1349), inside the read so a timeout still falls through to the peer
    // store. No sampling or probing happens here: each entry samples and
    // probes its own candidates.
    let discovery = fetch::discovery_options(common)?;
    let nodes = select_nodes(&chain.config, &chain.site, &discovery).await?;
    let signer = load_buyer_signer(chain)?;
    let funding = match &grant {
        None => Funding::SelfOwned {
            owner: signer,
            deposit: chain.deposit_policy(false)?,
        },
        Some(grant) => Funding::Delegated {
            slot: fetch::delegated_slot(grant, signer)?,
        },
    };

    let observer = Arc::new(PullObserver::new(args.json, run_label(out)));
    // Outlives the puller's passes, so an `openPool` or `topUp` that a Ctrl-C
    // or a fault dropped before its receipt was read is still named.
    let escrows = UnconfirmedEscrows::default();
    let mut puller = BundlePuller::builder(
        chain.config.clone(),
        chain.site.clone(),
        endpoint.clone(),
        Arc::clone(store),
        funding,
        discovery,
        nodes,
    )
    .relays(relays.to_vec())
    .namespace(uri.namespace)
    .jobs(args.jobs.unwrap_or(DEFAULT_JOBS))
    .max_lane_streams(usize::from(
        args.max_lane_streams.unwrap_or(DEFAULT_MAX_LANE_STREAMS),
    ))
    .max_rate_per_mb(common.max_rate_per_mb)
    .max_blob_bytes(common.max_blob_mb.saturating_mul(1024 * 1024))
    .stop(std::io::stderr().is_terminal(), common.give_up_after())
    .observer(Arc::clone(&observer) as Arc<dyn BundleObserver>)
    .fault_hints(Arc::new(CliFaultHints))
    .unconfirmed_escrows(escrows.clone())
    .build()
    .await?;

    // Watched from before the manifest: the manifest fetch is paid, so it can
    // open the run's pool. The first Ctrl-C stops the run where it is; a
    // second one exits at once.
    let mut interrupt = Interrupt::watch(escrows.clone());
    let result = pull_manifest(
        &mut puller,
        &mut interrupt,
        &observer,
        (args, out),
        uri,
        (filter, filters_given),
    )
    .await;
    // Every exit of the manifest or the pull can drop an `openPool` or a
    // `topUp` before its receipt is read, and the transaction may still escrow.
    let result = name_unconfirmed_escrows(result, &escrows.pending());
    // On every exit, a Ctrl-C's included: what the entries queued is durable
    // before the command returns.
    puller.finish().await;
    result
}

/// Print the run's wallet-shortfall warning, if it has one, for a run that
/// ends before its summary.
fn warn_shortfall(puller: &BundlePuller) {
    if let Some(shortfall) = puller.funding_shortfall() {
        eprintln!("warning: {shortfall}");
    }
}

/// Obtain the bundle manifest through `puller`, pull every kept entry, and
/// print the run summary.
async fn pull_manifest(
    puller: &mut BundlePuller,
    interrupt: &mut Interrupt,
    observer: &PullObserver,
    (args, out): (&GetArgs, &Path),
    uri: &DecdnUri,
    (filter, filters_given): (&EntryFilter, bool),
) -> anyhow::Result<()> {
    // Obtain the manifest: the bundle blob fetched and filtered here. `None` =>
    // filtered to empty (already reported). `--select`: let the user trim the
    // (already glob-filtered) list in their editor. Everything deselected ends
    // the run (reported) like an empty filter.
    let selected = async {
        let obtained = tokio::select! {
            obtained = obtain_manifest(puller, (args, out), uri, filter, filters_given) => {
                obtained?
            }
            () = interrupt.wait() => return Err(Interrupted.into()),
        };
        let Some(kept) = obtained else {
            return Ok(None);
        };
        let kept = maybe_select(args, out, uri, kept)?;
        if let Some(Kept { manifest, .. }) = &kept {
            refuse_foreign(args, out, manifest)?;
        }
        anyhow::Ok(kept)
    }
    .await;
    // The manifest fetch is paid, so a run that ends here still owes the
    // wallet-shortfall warning the summary below prints otherwise.
    let Some(Kept { manifest, excluded }) = selected.inspect_err(|_| warn_shortfall(puller))?
    else {
        warn_shortfall(puller);
        return Ok(());
    };

    // The first Ctrl-C cancels the pull, which stops every fetch where it is
    // and still reports what landed.
    let cancel = CancellationToken::new();
    let outcome = {
        let watch = async {
            interrupt.wait().await;
            cancel.cancel();
            std::future::pending::<std::convert::Infallible>().await
        };
        tokio::select! {
            outcome = puller.pull(&manifest, out, args.overwrite, &cancel) => outcome,
            never = watch => match never {},
        }
    };
    if let Some(replaced) = outcome.replaced {
        print_reclaim_note(replaced);
    }
    observer.finish();
    warn_leftover_partials(out, &manifest.entries, uri.hash);
    let reported = report(&outcome.report, excluded, out, args.json);
    // A fault that ended the whole pull is its result, so `main` maps it to
    // its exit code (75 for a give-up with no `openPool` in flight, 1
    // otherwise).
    match outcome.end {
        BundleEnd::Interrupted => Err(Interrupted.into()),
        BundleEnd::FundingNeeded { error, .. } | BundleEnd::Stopped(error) => Err(error),
        _ => reported,
    }
}

/// Name the pool a funding recovery step closed, which may still hold a
/// balance, and the command that reclaims it.
fn print_reclaim_note(replaced: PoolReplaced) {
    eprintln!(
        "note: pool {} accepts no more funds and may still hold a balance; once its \
         dispute window ends, reclaim it with: decdn pool reclaim --pool {}",
        replaced.closed, replaced.closed
    );
}

/// The kept manifest for the run: the bundle blob fetched and filtered here.
/// The fetch reads the output root's manifest cache first, unless
/// `--overwrite`. `Ok(None)` means the manifest is empty after filtering —
/// [`report_nothing_to_fetch`] was already called, so the caller returns
/// `Ok(())`.
async fn obtain_manifest(
    puller: &BundlePuller,
    (args, out): (&GetArgs, &Path),
    uri: &DecdnUri,
    filter: &EntryFilter,
    filters_given: bool,
) -> anyhow::Result<Option<Kept>> {
    let mut m = puller
        .fetch_manifest(uri.hash, out, !args.overwrite)
        .await
        .map_err(not_a_manifest_hint)?;
    let raw_empty = m.entries.is_empty();
    let (entries, excluded) = filter.apply_and_warn(m.entries)?;
    m.entries = entries;
    if m.entries.is_empty() {
        report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
        warn_leftover_partials(out, &[], uri.hash);
        return Ok(None);
    }
    Ok(Some(Kept {
        manifest: m,
        excluded,
    }))
}

/// The CLI's [`BundleObserver`]: the multi-bar progress renderer, built once
/// the run's totals are known; the `-v` line for each landed entry; and a
/// `warning:` or `note:` line on stderr for what does not fail the pull.
struct PullObserver {
    json: bool,
    label: String,
    progress: OnceLock<PullProgress>,
}

impl PullObserver {
    /// An observer that renders bars unless `json`, under the header `label`.
    const fn new(json: bool, label: String) -> Self {
        Self {
            json,
            label,
            progress: OnceLock::new(),
        }
    }

    /// Clear the bars at the end of the run; the command then prints its
    /// summary.
    fn finish(&self) {
        if let Some(progress) = self.progress.get() {
            progress.finish();
        }
    }
}

impl BundleObserver for PullObserver {
    fn run_started(&self, download_total: Option<u64>, content_total: Option<u64>) {
        // The kept manifest is known: enable the renderer (silent off a
        // terminal or under `--json`). Its total bar meters the run's
        // download, with the whole on-disk content size in the header.
        let _ = self.progress.set(PullProgress::new(
            self.json,
            download_total,
            content_total,
            &self.label,
        ));
    }

    fn file_started(
        &self,
        paths: &[String],
        download_total: u64,
        reconstruct_total: u64,
    ) -> Arc<dyn FileObserver> {
        let label = pull_progress::file_label(paths);
        let bar = match self.progress.get() {
            Some(progress) => progress.file_bar(&label, download_total, reconstruct_total),
            None => PullProgress::disabled().file_bar(&label, download_total, reconstruct_total),
        };
        Arc::new(bar)
    }

    fn entry_landed(&self, landed: &EntryLanded) {
        let bytes = EntryBytes {
            paid: landed.paid,
            spliced: landed.spliced,
            resumed: landed.resumed,
        };
        tracing::info!(
            "{}",
            entry_done_line(landed.hash, landed.size, bytes, landed.elapsed)
        );
    }

    fn warning(&self, warning: &str) {
        eprintln!("warning: {warning}");
    }

    fn pool_replaced(&self, replaced: &PoolReplaced) {
        eprintln!("note: {replaced}");
    }
}

/// The CLI's [`FaultHints`]: the remedies that name its own flags and
/// commands ([`fetch::annotate`]).
struct CliFaultHints;

impl FaultHints for CliFaultHints {
    fn annotate(&self, err: anyhow::Error, at: &FaultContext) -> anyhow::Error {
        fetch::annotate(err, at)
    }
}

/// The content bytes one landed entry paid for, spliced from disk, and
/// resumed from an earlier run. No byte counts in two of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EntryBytes {
    paid: u64,
    spliced: u64,
    resumed: u64,
}

/// A finished entry's line at `-v` (#2120): its size, the time it took, and
/// its rate over the bytes this run paid for. When the entry resumed bytes
/// from an earlier run or spliced bytes from disk, the line reports each
/// non-zero one apart from the paid bytes (#2189, #2236). The time covers the
/// whole entry: probing, every drive, any splice and the whole-file check.
fn entry_done_line(
    hash: [u8; 32],
    size: u64,
    bytes: EntryBytes,
    elapsed: std::time::Duration,
) -> String {
    let secs = elapsed.as_secs_f64();
    let rate = if secs > 0.0 {
        fetch::fmt_rate(fetch::bytes_as_f64(bytes.paid) / secs)
    } else {
        fetch::fmt_rate(0.0)
    };
    let detail = if bytes.spliced == 0 && bytes.resumed == 0 {
        rate
    } else {
        let mut parts = vec![format!(
            "{} downloaded at {rate}",
            indicatif::HumanBytes(bytes.paid)
        )];
        if bytes.resumed > 0 {
            parts.push(format!("{} resumed", indicatif::HumanBytes(bytes.resumed)));
        }
        if bytes.spliced > 0 {
            parts.push(format!(
                "{} spliced from disk",
                indicatif::HumanBytes(bytes.spliced)
            ));
        }
        parts.join(", ")
    };
    format!(
        "get: {}: {} in {secs:.1}s ({detail})",
        blake3::Hash::from_bytes(hash).to_hex(),
        indicatif::HumanBytes(size),
    )
}

/// Why a pull ended with no entries to fetch — picks the message
/// [`report_nothing_to_fetch`] prints, so the operator learns the actual cause
/// (an empty bundle, globs that matched nothing, or a fully deselected list)
/// rather than mistaking one for another.
#[derive(Clone, Copy)]
enum NothingReason {
    /// The bundle manifest itself is empty.
    EmptyBundle,
    /// `--include`/`--exclude` removed every entry.
    Filtered,
    /// The user deselected every entry in the `--select` editor.
    Deselected,
}

impl NothingReason {
    /// The reason for an empty result of a filter pass: [`Filtered`] when a filter
    /// was given and the bundle was non-empty before it, else [`EmptyBundle`].
    ///
    /// [`Filtered`]: NothingReason::Filtered
    /// [`EmptyBundle`]: NothingReason::EmptyBundle
    const fn from_filter(filters_given: bool, raw_empty: bool) -> Self {
        if filters_given && !raw_empty {
            Self::Filtered
        } else {
            Self::EmptyBundle
        }
    }
}

fn report_nothing_to_fetch(reason: NothingReason) {
    let msg = match reason {
        NothingReason::EmptyBundle => "bundle has no entries; nothing to fetch",
        NothingReason::Filtered => {
            "no bundle entries match the include/exclude filters; nothing to fetch"
        }
        NothingReason::Deselected => "every file was deselected; nothing to fetch",
    };
    println!("{msg}");
}

/// The hint for a bundle URI whose hash names a blob that is not a manifest:
/// the trailing `/` asked for a bundle, and dropping it fetches the raw blob.
/// Every other error passes through unchanged.
fn not_a_manifest_hint(err: anyhow::Error) -> anyhow::Error {
    if err
        .chain()
        .any(<dyn std::error::Error + 'static>::is::<NotAManifest>)
    {
        err.context(
            "the URI's trailing `/` names a bundle, but this blob is not a bundle manifest; \
             drop the trailing `/` to fetch the raw blob",
        )
    } else {
        err
    }
}

/// Print the would-fetch plan and exit (no network/chain/keystore activity).
/// The plan lists the entries of the bundle's manifest as cached in the output
/// root by an earlier pull. The cache is read even under `--overwrite`, which
/// makes a real pull fetch the manifest again: the cache is content-addressed,
/// so its bytes are exactly what that fetch returns. The entry selection
/// `filter` applies to those entries, so the plan is the entry set a real run
/// starts from and the unmatched-pattern warnings are the ones it prints. A real
/// run then skips files already present in the output root (unless
/// `--overwrite`) and warns on leftover partials; the dry run does neither. It
/// does refuse a default destination that holds a file the pull may replace
/// ([`refuse_foreign`]), as a real run does. A bundle with no cached manifest
/// reports only the bundle and the output root, with `entries: null` under
/// `--json`.
fn dry_run(
    args: &GetArgs,
    output: &Path,
    uri: &DecdnUri,
    filter: &EntryFilter,
    filters_given: bool,
) -> anyhow::Result<()> {
    let out = output.display();
    let cached = BundleManifest::cached(output, uri.hash).map_err(not_a_manifest_hint)?;
    let Some(mut manifest) = cached else {
        let h = blake3::Hash::from_bytes(uri.hash).to_hex();
        if args.json {
            let plan = serde_json::json!({
                "output": out.to_string(),
                "hash": h.as_str(),
                "count": null,
                "entries": null,
            });
            println!("{plan}");
            return Ok(());
        }
        let filter_note = if filters_given {
            "; the include/exclude patterns are matched against them then"
        } else {
            ""
        };
        println!(
            "--dry-run: would fetch bundle {h} then its entries into {out} \
             (no cached manifest: entries are listed once a pull into this directory \
             caches it{filter_note})"
        );
        return Ok(());
    };
    let raw_empty = manifest.entries.is_empty();
    manifest.entries = filter.apply_and_warn(manifest.entries)?.0;
    refuse_foreign(args, output, &manifest)?;
    if args.json {
        // The `--json` plan stays machine-readable — an empty set is
        // `count: 0` with an empty `entries` array, no prose line. A bundle
        // with no cached manifest gives `count`/`entries` of `null`.
        let plan = serde_json::json!({
            "output": out.to_string(),
            "count": manifest.entries.len(),
            "entries": manifest.entries.iter().map(|e| serde_json::json!({
                "path": e.path, "hash": e.hash, "size": e.size,
                "chunks": e.chunks.as_ref().map(Vec::len),
            })).collect::<Vec<_>>(),
        });
        println!("{plan}");
    } else if manifest.entries.is_empty() {
        // Match the real run's empty-result message rather than printing a
        // "would fetch 0 entr(ies)" plan, so `--dry-run` and a live pull
        // agree on what an emptied set looks like.
        report_nothing_to_fetch(NothingReason::from_filter(filters_given, raw_empty));
    } else {
        println!(
            "would fetch {} entr(ies) into {out}:",
            manifest.entries.len()
        );
        for e in &manifest.entries {
            let chunks = e
                .chunks
                .as_ref()
                .map(|c| format!(", {} chunks", c.len()))
                .unwrap_or_default();
            match e.size {
                Some(n) => println!("  {} ({n} bytes{chunks})", e.path),
                None => println!("  {}{}", e.path, chunks),
            }
        }
    }
    Ok(())
}

/// Summarize the run; return an error if any entry failed (after reporting
/// all). Each of the report's warnings (a blob whose size differs from its
/// manifest, a wallet short of USDC) prints one line on stderr and never
/// changes the result. `excluded` is the count of entries the run does not
/// pull ([`PullReport`]).
fn report(rep: &BundleReport, excluded: u64, output: &Path, json: bool) -> anyhow::Result<()> {
    for failure in &rep.failures {
        // A per-entry failure is a command result the user needs, not
        // routing narration: keep it on stderr (unconditional, and clear
        // of the `--json` report on stdout) rather than behind logging.
        eprintln!("failed: {}: {}", failure.path, failure.error);
    }
    for line in &rep.warnings {
        eprintln!("warning: {line}");
    }

    let summary = pull_report(rep, excluded, output);
    if json {
        let line = serde_json::to_string(&summary).map_err(|e| anyhow!("serialize report: {e}"))?;
        println!("{line}");
    } else {
        println!("{}", counts_line(&summary));
        // `downloaded X (paid P) → reconstructed Y`: the arrow half only when
        // dedup made the two differ, the paid part unless the run neither
        // downloaded nor paid.
        println!(
            "{}",
            transfer_line(rep.downloaded, rep.paid_bytes, rep.reconstructed)
        );
        // The bytes earlier, interrupted runs fetched and this run resumed, shown
        // only when a run resumed any: they are not in `downloaded` or `paid`.
        if rep.resumed_bytes > 0 {
            println!(
                "resumed {} from earlier partials",
                human_bytes(rep.resumed_bytes)
            );
        }
        // The whole-file dedup outcome, shown only when it mattered: a run that
        // materialized any destination from an on-disk donor instead of fetching.
        if rep.reused > 0 {
            println!(
                "whole-file dedup: reused {} from disk ({} file(s))",
                human_bytes(rep.reused_bytes),
                rep.reused
            );
        }
        // The range-dedup outcome, shown only when a hint mattered: a run that
        // spliced bytes from disk, or one whose hints were dropped by a fault. A
        // plain pull with no usable hints stays silent (both are zero), so a
        // hint-carrying bundle whose hints all lie (spliced 0, some ignored)
        // does not print the same summary as an unhinted one.
        if rep.spliced_bytes > 0 || rep.hints_ignored > 0 {
            println!(
                "range-dedup: spliced {} from disk, {} hint(s) ignored",
                human_bytes(rep.spliced_bytes),
                rep.hints_ignored
            );
        }
    }

    if rep.failed > 0 {
        bail!("{} entr(ies) failed to fetch", rep.failed);
    }
    Ok(())
}

/// The run's [`PullReport`]: `rep`'s counts and byte tallies, beside the
/// `excluded` count and the output directory the command holds.
fn pull_report(rep: &BundleReport, excluded: u64, output: &Path) -> PullReport {
    PullReport {
        output: output.display().to_string(),
        fetched: rep.fetched,
        linked: rep.linked,
        skipped: rep.skipped,
        excluded,
        failed: rep.failed,
        downloaded: rep.downloaded,
        paid_bytes: rep.paid_bytes,
        reconstructed: rep.reconstructed,
        spliced_bytes: rep.spliced_bytes,
        hints_ignored: rep.hints_ignored,
        resumed_bytes: rep.resumed_bytes,
        reused: rep.reused,
        reused_bytes: rep.reused_bytes,
    }
}

/// The summary's first line: where the run pulled to, and every entry count.
fn counts_line(rep: &PullReport) -> String {
    format!(
        "pulled into {} ({} fetched, {} linked, {} skipped, {} reused, {} excluded, {} failed)",
        rep.output, rep.fetched, rep.linked, rep.skipped, rep.reused, rep.excluded, rep.failed
    )
}

/// Scan `<out_root>/.decdn-partial` for leftover per-hash staging files —
/// `<hex>`, `<hex>.partial`, `<hex>.partial.ranges` — whose hash is not in
/// `keep`, and total the disk space they take ([`allocated_bytes`]). Other
/// names (a record's temporary file) are not counted. A missing or unreadable
/// directory holds nothing.
fn leftover_partials(out_root: &Path, keep: &HashSet<[u8; 32]>) -> Leftovers {
    let Ok(dir) = std::fs::read_dir(out_root.join(STAGING_DIR)) else {
        return Leftovers::default();
    };
    let mut blobs: HashSet<[u8; 32]> = HashSet::new();
    let mut bytes = 0u64;
    for entry in dir.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let stem = name
            .strip_suffix(".partial.ranges")
            .or_else(|| name.strip_suffix(".partial"))
            .unwrap_or(name);
        let Ok(hash) = blake3::Hash::from_hex(stem) else {
            continue;
        };
        let hash = *hash.as_bytes();
        if keep.contains(&hash) {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_file() {
            blobs.insert(hash);
            bytes = bytes.saturating_add(allocated_bytes(&meta));
        }
    }
    Leftovers {
        blobs: u64::try_from(blobs.len()).unwrap_or(u64::MAX),
        bytes,
    }
}

/// The disk space a file takes. A range-dedup `.partial` is sized to the whole
/// blob before any byte lands, so its length can be far above the space that
/// deleting it frees. On Unix the allocated blocks give that space; elsewhere
/// the length is the best figure.
fn allocated_bytes(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}

/// Staging files in the staging directory that a run did not use: how many
/// distinct blobs, and the disk space their files take.
#[derive(Debug, Default, PartialEq, Eq)]
struct Leftovers {
    blobs: u64,
    bytes: u64,
}

/// The warning line for `leftovers` in `dir`, without the `warning: ` prefix,
/// or `None` when there are none.
fn leftover_warning(dir: &Path, leftovers: &Leftovers) -> Option<String> {
    let size = human_bytes(leftovers.bytes);
    let dir = dir.display();
    match leftovers.blobs {
        0 => None,
        1 => Some(format!(
            "1 partial download ({size} on disk) that this run did not use remains in \
             {dir}; delete it to reclaim the space"
        )),
        n => Some(format!(
            "{n} partial downloads ({size} on disk) that this run did not use remain in \
             {dir}; delete them to reclaim the space"
        )),
    }
}

/// Warn on stderr about leftover staging files of blobs this run did not use
/// ([`leftover_partials`]): a partial of an entry an earlier run started,
/// which this run's filter or selection left out, or one another bundle
/// pulled into the same output directory left. They stay for a later run that
/// selects the entry again, so the warning only names them and never deletes
/// them. `entries` are the run's selected entries (none when the filter or
/// `--select` emptied the run), and `bundle_hash` is the manifest blob, which
/// is also this run's own.
fn warn_leftover_partials(out_root: &Path, entries: &[ManifestEntry], bundle_hash: [u8; 32]) {
    let keep: HashSet<[u8; 32]> = entries
        .iter()
        .filter_map(|e| fetch::parse_hash(&e.hash).ok())
        .chain([bundle_hash])
        .collect();
    let leftovers = leftover_partials(out_root, &keep);
    if let Some(line) = leftover_warning(&out_root.join(STAGING_DIR), &leftovers) {
        eprintln!("warning: {line}");
    }
}

/// Format a byte count as a short decimal-unit label (`13.8 GB`). SI (1000-based)
/// units match how file and model sizes are usually quoted.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if n < 1000 {
        return format!("{n} B");
    }
    // Precision loss is irrelevant here: the result is a one-decimal human label.
    #[allow(
        clippy::cast_precision_loss,
        reason = "display-only size label; exact integer value is not needed"
    )]
    let mut value = n as f64;
    let mut unit = 0usize;
    while value >= 1000.0 && unit + 1 < UNITS.len() {
        value /= 1000.0;
        unit += 1;
    }
    let label = UNITS.get(unit).copied().unwrap_or("B");
    format!("{value:.1} {label}")
}

/// The end-of-pull transfer line. `downloaded X` carries the wire bytes the
/// run paid for as `(paid P)`, left out only when the run neither downloaded
/// nor paid anything. `paid` is wire bytes and `downloaded` content bytes, so
/// on a clean pull `paid` runs ahead by the bao proof overhead. When dedup
/// saved a transfer, `downloaded` and `reconstructed`
/// differ and both are shown with an arrow; otherwise the `→ reconstructed`
/// half is omitted rather than repeating the same figure.
fn transfer_line(downloaded: u64, paid: u64, reconstructed: u64) -> String {
    let paid = if downloaded > 0 || paid > 0 {
        format!(" (paid {})", human_bytes(paid))
    } else {
        String::new()
    };
    let rebuilt = if downloaded == reconstructed {
        String::new()
    } else {
        format!(" → reconstructed {}", human_bytes(reconstructed))
    };
    format!("downloaded {}{paid}{rebuilt}", human_bytes(downloaded))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
