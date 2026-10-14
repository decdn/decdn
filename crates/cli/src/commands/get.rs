//! `decdn get <URI>` (#2449): routes a `decdn:` URI to the blob fetch or the
//! bundle pull.
//!
//! The command adds no pull behavior. It parses the URI with
//! [`decdn_client::uri::DecdnUri`], checks the flags against the URI's target, and
//! hands the same arguments `decdn fetch` or `decdn bundle pull` takes to that
//! command:
//!
//! - `decdn:<hash>` runs [`super::fetch::fetch`].
//! - `decdn:<hash>/` runs [`super::bundle_pull::bundle_pull`] over every entry.
//! - `decdn:<hash>/<path>` runs the bundle pull with one `--include` glob that
//!   matches exactly `<path>`.
//!
//! The URI's `ns` is the namespace hint of either route.

use std::path::Path;

use anyhow::Context as _;
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::cli::bundle::{DEFAULT_JOBS, DEFAULT_MAX_LANE_STREAMS};
use decdn_common::cli::{BundlePullArgs, FetchArgs, GetArgs};

/// The command a `decdn get` runs, with the arguments it runs it with.
#[derive(Debug)]
enum Route {
    /// The blob fetch.
    Fetch(Box<FetchArgs>),
    /// The bundle pull, over every entry or one.
    Bundle(Box<BundlePullArgs>),
}

/// Run `decdn get`: parse the URI and run the fetch or bundle pull it names.
///
/// # Errors
///
/// The URI does not parse, a flag does not apply to the URI's target, or the
/// routed command fails.
pub async fn get(args: &GetArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    match route(args)? {
        Route::Fetch(fetch_args) => super::fetch::fetch(&fetch_args, config_path).await,
        Route::Bundle(pull_args) => super::bundle_pull::bundle_pull(&pull_args, config_path).await,
    }
}

/// The command `args` routes to. Rejects a flag that does not apply to the
/// URI's target: any bundle flag on a blob URI, an entry filter on an entry
/// URI, and `-o -` on anything but a blob URI.
fn route(args: &GetArgs) -> anyhow::Result<Route> {
    let uri: DecdnUri = args
        .uri
        .parse()
        .with_context(|| format!("parse URI {:?}", args.uri))?;
    let hash = blake3::Hash::from_bytes(uri.hash).to_hex().to_string();
    let include = match uri.target {
        UriTarget::Blob => {
            let given = bundle_flags_given(args);
            anyhow::ensure!(
                given.is_empty(),
                "{} only apply to a bundle URI (`decdn:<hash>/`), not a blob URI",
                given.join(", ")
            );
            return Ok(Route::Fetch(Box::new(FetchArgs {
                hash,
                output: args.output.clone(),
                namespace: uri.namespace,
                common: args.common.clone(),
            })));
        }
        UriTarget::Bundle => args.include.clone(),
        UriTarget::BundleEntry(path) => {
            let given = entry_filters_given(args);
            anyhow::ensure!(
                given.is_empty(),
                "{} do not apply to an entry URI: the URI already names one entry",
                given.join(", ")
            );
            vec![literal_glob(&path)]
        }
        _ => anyhow::bail!("unsupported URI target in {}", args.uri),
    };
    anyhow::ensure!(
        args.output != Path::new("-"),
        "`-o -` (stdout) takes a blob URI (`decdn:<hash>`); a bundle needs an output directory"
    );
    Ok(Route::Bundle(Box::new(BundlePullArgs {
        input: None,
        hash: Some(hash),
        output: args.output.clone(),
        jobs: args.jobs.unwrap_or(DEFAULT_JOBS),
        max_lane_streams: args.max_lane_streams.unwrap_or(DEFAULT_MAX_LANE_STREAMS),
        include,
        exclude: args.exclude.clone(),
        select: args.select,
        overwrite: args.overwrite,
        dry_run: args.dry_run,
        json: args.json,
        namespace: uri.namespace,
        common: args.common.clone(),
    })))
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

/// An `--include` glob that matches exactly the manifest path `path`. Glob
/// metacharacters in a real filename match themselves. A backslash is the
/// glob escape character on Unix, so it is escaped there too.
fn literal_glob(path: &str) -> String {
    if cfg!(windows) {
        globset::escape(path)
    } else {
        globset::escape(&path.replace('\\', r"\\"))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
