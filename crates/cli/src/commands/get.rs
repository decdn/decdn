//! `decdn get <URI>` (#2449): pull what a `decdn:` URI names
//! (`adr/appendix-bundles.md` § `decdn:` URIs).
//!
//! The command parses the URI with [`decdn_client::uri::DecdnUri`], checks the
//! flags against the URI's target, and runs the route for that target:
//!
//! - `decdn:<hash>` runs the blob fetch, `fetch::fetch`.
//! - `decdn:<hash>/` runs the bundle pull, `bundle_pull::bundle_pull`, over
//!   every entry.
//! - `decdn:<hash>/<path>` runs the bundle pull for exactly that entry. With
//!   `-o -` it runs the blob fetch instead, which reads the manifest and
//!   streams that entry's blob to stdout.
//!
//! The URI's `ns` is the namespace hint of either route.
//!
//! Without `-o`, the destination follows the target: `./<hash>` for a blob,
//! `.` for a bundle, and `./<name>` for an entry, where `<name>` is the last
//! segment of the entry's path. The command then writes the absolute
//! destination to stderr before the pull starts, except under `--json` or
//! `--dry-run`. A bundle or entry pull into a default destination refuses a
//! path that holds a file no earlier pull of the same entry left there
//! unchanged, unless `--overwrite`.
//!
//! A default destination, like any relative `-o`, resolves against the
//! current directory, and so does the run's resume state. A run that stops on
//! Ctrl-C or a give-up names that directory as the one to run the command again
//! from.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::GetArgs;

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
    let result = if streams_one_blob(args, &uri) {
        super::fetch::fetch(args, &dest.path, &uri, config_path).await
    } else {
        super::bundle_pull::bundle_pull(args, &dest, &uri, config_path).await
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

/// Where a `decdn get` run writes: `-o`, or the default for the URI's target.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Destination {
    /// The blob's destination file, or the bundle's output directory.
    pub(crate) path: PathBuf,
    /// The file name the entry an entry URI names lands at, directly in
    /// `path`, in place of its full manifest path under `path`. Set only for
    /// an entry URI without `-o`.
    pub(crate) entry_name: Option<String>,
}

impl Destination {
    /// The destination of `args` for `uri`: `-o` as given, or else
    /// `./<hash>` for a blob, `.` for a bundle, and `.` for an entry with the
    /// entry landing at the last segment of its path.
    pub(crate) fn of(args: &GetArgs, uri: &DecdnUri) -> Self {
        if let Some(output) = &args.output {
            return Self {
                path: output.clone(),
                entry_name: None,
            };
        }
        match &uri.target {
            UriTarget::Blob => Self {
                path: Path::new(".").join(blake3::Hash::from_bytes(uri.hash).to_hex().as_str()),
                entry_name: None,
            },
            // The URI parser holds every segment non-empty and free of `/`,
            // so the last segment is a plain name. The pull still checks it
            // against the path-safety rules, like any manifest path.
            UriTarget::BundleEntry(entry) => Self {
                path: PathBuf::from("."),
                entry_name: Some(
                    entry
                        .rsplit_once('/')
                        .map_or(entry.as_str(), |(_, name)| name)
                        .to_owned(),
                ),
            },
            _ => Self {
                path: PathBuf::from("."),
                entry_name: None,
            },
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
/// a flag that does not apply: any bundle flag on a blob URI, or on an entry
/// URI to stdout; an entry filter on an entry URI; and `-o -` on a whole
/// bundle.
fn parse_and_check(args: &GetArgs) -> anyhow::Result<DecdnUri> {
    let uri: DecdnUri = args
        .uri
        .parse()
        .with_context(|| format!("parse URI {:?}", args.uri))?;
    match &uri.target {
        UriTarget::Blob => {
            let given = bundle_flags_given(args);
            anyhow::ensure!(
                given.is_empty(),
                "{} only apply to a bundle URI (`decdn:<hash>/`), not a blob URI",
                given.join(", ")
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
        _ => {}
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
