//! `decdn bundle` — the `pull` subcommand group. Issue #391.
//!
//! Bundle manifests are produced by `decdn origin import --dry-run` (see
//! [`super::manifest`] and [`super::origin`]); this module only realizes
//! them, fetching every referenced blob over the paid `cdn/client/v1` path.

use std::path::Path;

use decdn_common::cli::{BundleArgs, BundleCommand};

/// Top-level dispatcher for `decdn bundle ...`. `config_path` (the global
/// `--config`) is consumed by `pull` for relays/discovery/chain coordinates.
pub async fn bundle_dispatch(args: &BundleArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    match &args.cmd {
        BundleCommand::Pull(pull_args) => {
            super::bundle_pull::bundle_pull(pull_args, config_path).await
        }
    }
}
