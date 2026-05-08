//! `decdn node top` — live view of node activity scraped from the
//! daemon's loopback `/metrics` HTTP endpoint (issue #275).

use std::path::Path;

use decdn_common::cli;

/// Entry point dispatched from `node_dispatch`. Currently a stub —
/// later tasks add the metrics fetch, parse, and render loop. The
/// signature is `async` because the dispatch arm awaits it; clippy
/// would otherwise flag the missing await on this scaffolding.
#[allow(clippy::unused_async)]
pub async fn run(_args: &cli::TopArgs, _global_config: Option<&Path>) -> anyhow::Result<()> {
    anyhow::bail!("decdn node top is not yet implemented")
}
