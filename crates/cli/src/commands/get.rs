//! `decdn get <URI>` (#2449): pull what a `decdn:` URI names
//! (`adr/appendix-bundles.md` § `decdn:` URIs).
//!
//! The command parses the URI with [`decdn_client::uri::DecdnUri`], checks the
//! flags against the URI's target, and runs the route for that target:
//!
//! - `decdn:<hash>` runs the blob fetch, `fetch::fetch`.
//! - `decdn:<hash>/` runs the bundle pull, `bundle_pull::bundle_pull`, over
//!   every entry.
//! - `decdn:<hash>/<path>` runs the bundle pull for exactly that entry.
//!
//! The URI's `ns` is the namespace hint of either route.
//!
//! Without `-o`, the destination follows the target: `./<hash>` for a blob,
//! `.` for a bundle, and `./<name>` for an entry, where `<name>` is the last
//! segment of the entry's path. The command then names the destination on
//! stderr before the pull starts.

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
        eprintln!("Downloading to {}", dest.shown(&uri).display());
    }
    match uri.target {
        UriTarget::Blob => super::fetch::fetch(args, &dest.root, &uri, config_path).await,
        _ => super::bundle_pull::bundle_pull(args, &dest, &uri, config_path).await,
    }
}

/// Where a `decdn get` run writes: `-o`, or the default for the URI's target.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Destination {
    /// The blob's destination file, or the bundle's output root.
    pub(crate) root: PathBuf,
    /// Whether the entry an entry URI names lands at its file name directly in
    /// `root`, not at its full path under `root`. Set only without `-o`.
    pub(crate) flatten: bool,
}

impl Destination {
    /// The destination of `args` for `uri`: `-o` as given, or else
    /// `./<hash>` for a blob, and `.` for a bundle or an entry, with the entry
    /// flattened to its file name.
    pub(crate) fn of(args: &GetArgs, uri: &DecdnUri) -> Self {
        if let Some(output) = &args.output {
            return Self {
                root: output.clone(),
                flatten: false,
            };
        }
        match &uri.target {
            UriTarget::Blob => Self {
                root: Path::new(".").join(blake3::Hash::from_bytes(uri.hash).to_hex().as_str()),
                flatten: false,
            },
            UriTarget::BundleEntry(_) => Self {
                root: PathBuf::from("."),
                flatten: true,
            },
            _ => Self {
                root: PathBuf::from("."),
                flatten: false,
            },
        }
    }

    /// The path to name for the user: the entry's file for a flattened entry,
    /// and `root` otherwise.
    fn shown(&self, uri: &DecdnUri) -> PathBuf {
        match &uri.target {
            UriTarget::BundleEntry(path) if self.flatten => Path::new(path)
                .file_name()
                .map_or_else(|| self.root.clone(), |name| self.root.join(name)),
            _ => self.root.clone(),
        }
    }
}

/// The URI `args` names, once its flags are checked against its target. Rejects
/// a flag that does not apply: any bundle flag on a blob URI, an entry filter
/// on an entry URI, and `-o -` on anything but a blob URI.
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
        }
        _ => {}
    }
    anyhow::ensure!(
        args.output.as_deref() != Some(Path::new("-")),
        "`-o -` (stdout) takes a blob URI (`decdn:<hash>`); a bundle needs an output directory"
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
