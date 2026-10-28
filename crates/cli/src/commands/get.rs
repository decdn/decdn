//! `decdn get <URI>` (#2449): pull what a `decdn:` URI names
//! (`adr/appendix-bundles.md` § `decdn:` URIs).
//!
//! The command parses the URI with [`decdn_client::uri::DecdnUri`], checks the
//! flags against the URI's target, and runs the route for that target:
//!
//! - `decdn:<hash>` runs the blob fetch, `fetch::fetch`. With `--dry-run` it
//!   prints the blob's quote instead (`quote::dry_run_blob`).
//! - `decdn:<hash>/` runs the bundle pull, `bundle_pull::bundle_pull`, over
//!   every entry.
//! - `decdn:<hash>/<path>` runs the bundle pull for exactly that entry. With
//!   `-o -` it runs the blob fetch instead, which reads the manifest and
//!   streams that entry's blob to stdout.
//!
//! The URI's `ns` is the namespace hint of either route.
//!
//! Every route that paid ends with the run's pull receipt (`receipt`): the
//! block names what landed, the nodes that served it and what they were paid,
//! and `--receipt` writes the signed evidence to a file.
//!
//! Without `-o`, the destination follows the target: `./<hash>` for a blob,
//! `./<hash>/` for a bundle, and `./<name>` for an entry, where `<name>` is
//! the last segment of the entry's path. The command then writes the absolute
//! destination to stderr before the pull starts, except under `--json` or
//! `--dry-run`. A bundle or entry pull into a default destination refuses a
//! path that holds a file no earlier pull of the same entry left there
//! unchanged, unless `--overwrite`, and refuses a hidden path — one with a
//! `.`-prefixed component — outright; an explicit `-o` names the directory,
//! so it takes such paths unchanged.
//!
//! A default destination, like any relative `-o`, resolves against the
//! current directory, and so does the run's resume state. A run that stops on
//! Ctrl-C or a give-up names that directory as the one to run the command again
//! from.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use decdn_client::receipt::ReceiptEvidence;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::GetArgs;

use super::activity::ActivityRecorder;

/// Run `decdn get`: parse the URI and run the route its target names.
///
/// # Errors
///
/// The URI does not parse, a flag does not apply to the URI's target, or the
/// route fails.
pub async fn get(args: &GetArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let uri = parse_and_check(args)?;
    let dest = Destination::of(args, &uri);
    if args.output.is_none() && !args.json && !args.dry_run {
        let shown = dest.shown();
        let shown = std::path::absolute(&shown).unwrap_or(shown);
        eprintln!("Downloading to {}", shown.display());
    }
    if args.dry_run && uri.target == UriTarget::Blob {
        return super::quote::dry_run_blob(args, &dest.path, &uri, config_path).await;
    }
    // A dry run pays nothing and writes nothing: no receipt either way, and a
    // file at `--receipt` stays as it is.
    let receipt = args.receipt.as_deref().filter(|_| !args.dry_run);
    if let Some(path) = receipt {
        super::receipt::check_receipt_path(path)?;
        if streams_one_blob(args, &uri) && !is_stdout(args) {
            super::receipt::refuse_collision(path, &dest.path, "the blob lands")?;
        }
    }
    let started = super::receipt::unix_now();
    let evidence = Arc::new(ReceiptEvidence::new());
    // A dry run is no pull: it keeps no record for the dashboard.
    let activity = (!args.dry_run)
        .then(|| ActivityRecorder::start(&uri, &args.uri, &dest.path, &evidence))
        .flatten();
    let result = if streams_one_blob(args, &uri) {
        super::fetch::fetch(
            args,
            &dest.path,
            &uri,
            config_path,
            &evidence,
            activity.as_ref(),
        )
        .await
    } else {
        let records = (&evidence, activity.as_ref());
        super::bundle_pull::bundle_pull(args, &dest, &uri, config_path, records).await
    };
    let result = if args.dry_run {
        result
    } else {
        let (result, summary) =
            super::receipt::finish_get(args, &uri, &evidence, started, result).await;
        if let Some(activity) = &activity {
            activity.finish(&result, summary).await;
        }
        result
    };
    with_resume_hint(args, result)
}

/// `result` with the [`resume_dir`] of `args` attached to a stop, so its
/// re-run advice names the directory to run the command again from.
fn with_resume_hint<T>(args: &GetArgs, result: anyhow::Result<T>) -> anyhow::Result<T> {
    match resume_dir(args) {
        Some(dir) => super::interrupt::name_resume_dir(result, &dir),
        None => result,
    }
}

/// The directory a stopped run must run again from to resume: the current
/// directory, which a default destination or a relative `-o` resolves against.
/// `None` for an absolute `-o`, whose resume state a re-run from any directory
/// finds, for `-o -`, which keeps none, and when the current directory cannot
/// be read.
fn resume_dir(args: &GetArgs) -> Option<PathBuf> {
    if args.output.as_deref().is_some_and(Path::is_absolute) || is_stdout(args) {
        return None;
    }
    std::env::current_dir().ok()
}

/// The path a `decdn get` of `uri` without `-o` writes to: `./<hash>` for
/// a blob (the file) and for a whole bundle (its own directory), and `.` for
/// an entry (the directory its file lands in, at its name). A bundle's
/// manifest cache and resume state live in this directory, so `decdn quote`
/// reads and caches a manifest at the same default.
pub(crate) fn default_path(uri: &DecdnUri) -> PathBuf {
    match &uri.target {
        UriTarget::BundleEntry(_) => PathBuf::from("."),
        _ => Path::new(".").join(blake3::Hash::from_bytes(uri.hash).to_hex().as_str()),
    }
}

/// The name the entry an entry URI names lands at without `-o`: the last
/// segment of its path. `None` for any other target.
pub(crate) fn default_entry_name(uri: &DecdnUri) -> Option<String> {
    // The URI parser holds every segment non-empty and free of `/`, so an
    // entry's last segment is a plain name. The pull still checks it against
    // the path-safety rules, like any manifest path.
    match &uri.target {
        UriTarget::BundleEntry(entry) => Some(
            entry
                .rsplit_once('/')
                .map_or(entry.as_str(), |(_, name)| name)
                .to_owned(),
        ),
        _ => None,
    }
}

/// Refuse a whole-bundle pull of `uri` into its default directory `path`
/// ([`default_path`]) when `path` holds something that is not a directory.
/// A blob URI without `-o` writes its file at the same `./<hash>`, so the
/// file there is most likely the raw manifest of an earlier `decdn get` of
/// the URI without the trailing `/`. The pull could never create its
/// directory there, so this check runs before any network call or payment.
/// Other targets pass.
///
/// # Errors
///
/// When `uri` names a whole bundle and `path` exists and is not a directory
/// (a symlink counts by its target).
pub(crate) fn refuse_file_at_bundle_dir(uri: &DecdnUri, path: &Path) -> anyhow::Result<()> {
    if uri.target != UriTarget::Bundle || path.is_dir() || std::fs::symlink_metadata(path).is_err()
    {
        return Ok(());
    }
    let hex = blake3::Hash::from_bytes(uri.hash).to_hex();
    anyhow::bail!(
        "{path} exists and is not a directory, so the bundle cannot land in it: it is likely \
         the raw manifest that `decdn get decdn:{hex}` (without the trailing `/`) wrote there; \
         remove it to pull the bundle into {path}, or pass `-o <dir>` to pull it into another \
         directory",
        path = path.display(),
    )
}

/// Where a `decdn get` run writes: `-o`, or the default for the URI's target.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Destination {
    /// The blob's destination file, or the bundle's output directory.
    pub(crate) path: PathBuf,
    /// The file name the entry an entry URI names lands at, directly in
    /// `path`, in place of its full manifest path under `path`. Set only for
    /// an entry URI without `-o`.
    pub(crate) entry_name: Option<String>,
    /// Whether no `-o` was given: `path` is the target's default, a path the
    /// user did not name. The bundle pull's default-destination refusals read
    /// this field, not `args.output`.
    pub(crate) defaulted: bool,
}

impl Destination {
    /// The destination of `args` for `uri`: `-o` as given, or else
    /// `./<hash>` for a blob, `./<hash>/` for a bundle, and `.` for an entry
    /// with the entry landing at the last segment of its path.
    pub(crate) fn of(args: &GetArgs, uri: &DecdnUri) -> Self {
        if let Some(output) = &args.output {
            return Self {
                path: output.clone(),
                entry_name: None,
                defaulted: false,
            };
        }
        Self {
            path: default_path(uri),
            entry_name: default_entry_name(uri),
            defaulted: true,
        }
    }

    /// The path to name for the user: the entry's file when it lands at its
    /// name, and `path` otherwise.
    fn shown(&self) -> PathBuf {
        self.entry_name
            .as_ref()
            .map_or_else(|| self.path.clone(), |name| self.path.join(name))
    }
}

/// Whether the run delivers one blob: a blob URI, or an entry URI to stdout.
fn streams_one_blob(args: &GetArgs, uri: &DecdnUri) -> bool {
    match uri.target {
        UriTarget::Blob => true,
        UriTarget::BundleEntry(_) => is_stdout(args),
        _ => false,
    }
}

/// Whether `-o` is `-` (stdout).
fn is_stdout(args: &GetArgs) -> bool {
    args.output.as_deref() == Some(Path::new("-"))
}

/// The URI `args` names, once its flags are checked against its target. Rejects
/// a flag that does not apply: a bundle flag other than `--dry-run` on a blob
/// URI, and `--json` there without `--dry-run`; any bundle flag on an entry
/// URI to stdout; an entry filter on an entry URI; and `-o -` on a whole
/// bundle.
fn parse_and_check(args: &GetArgs) -> anyhow::Result<DecdnUri> {
    let uri: DecdnUri = args
        .uri
        .parse()
        .with_context(|| format!("parse URI {:?}", args.uri))?;
    match &uri.target {
        UriTarget::Blob => {
            let given: Vec<_> = bundle_flags_given(args)
                .into_iter()
                .filter(|flag| !matches!(*flag, "--dry-run" | "--json"))
                .collect();
            anyhow::ensure!(
                given.is_empty(),
                "{} only apply to a bundle URI (`decdn:<hash>/`), not a blob URI",
                given.join(", ")
            );
            anyhow::ensure!(
                !args.json || args.dry_run,
                "--json applies to a blob URI only with --dry-run"
            );
            return Ok(uri);
        }
        UriTarget::BundleEntry(_) => {
            let given = entry_filters_given(args);
            anyhow::ensure!(
                given.is_empty(),
                "{} do not apply to an entry URI: the URI already names one entry",
                given.join(", ")
            );
            if is_stdout(args) {
                let given = bundle_flags_given(args);
                anyhow::ensure!(
                    given.is_empty(),
                    "{} do not apply with `-o -`: an entry streams to stdout as one blob",
                    given.join(", ")
                );
            }
            return Ok(uri);
        }
        UriTarget::Bundle => {}
        _ => anyhow::bail!("unsupported URI target in {:?}", args.uri),
    }
    anyhow::ensure!(
        !is_stdout(args),
        "`-o -` (stdout) takes a blob or one bundle entry (`decdn:<hash>/<path>`); \
         a whole bundle writes to a directory"
    );
    Ok(uri)
}

/// The bundle-only flags `args` sets, by name.
fn bundle_flags_given(args: &GetArgs) -> Vec<&'static str> {
    let mut given = entry_filters_given(args);
    for (set, name) in [
        (args.jobs.is_some(), "--jobs"),
        (args.max_lane_streams.is_some(), "--max-lane-streams"),
        (args.overwrite, "--overwrite"),
        (args.dry_run, "--dry-run"),
        (args.json, "--json"),
    ] {
        if set {
            given.push(name);
        }
    }
    given
}

/// The entry filters `args` sets, by name.
fn entry_filters_given(args: &GetArgs) -> Vec<&'static str> {
    [
        (!args.include.is_empty(), "--include"),
        (!args.exclude.is_empty(), "--exclude"),
        (args.select, "--select"),
    ]
    .into_iter()
    .filter_map(|(set, name)| set.then_some(name))
    .collect()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
