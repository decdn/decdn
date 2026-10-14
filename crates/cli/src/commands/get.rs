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

use std::path::Path;

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
    match uri.target {
        UriTarget::Blob => super::fetch::fetch(args, &uri, config_path).await,
        _ => super::bundle_pull::bundle_pull(args, &uri, config_path).await,
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
        args.output != Path::new("-"),
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
