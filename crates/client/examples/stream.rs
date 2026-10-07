//! Stream one blob to stdout with the [`Streamer`] face.
//!
//! ```text
//! cargo run -p decdn-client --example stream -- <blake3-hash-hex> | tar x
//! ```
//!
//! Set the `DECDN_*` variables that `common::Env::from_env` lists first. Only
//! verified bytes reach stdout. The fetch runs at most one read-ahead window
//! ahead of the reader, so a slow or early-exiting consumer stops the pull, and
//! the spend, within that window.

mod common;

use std::sync::Arc;

use anyhow::{Context, Result};
use decdn_client::{NoCache, ProgressClock, PullConfig, StaticSources, StopPolicy, Streamer};

use common::{Buyer, Env, NoTopUp};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let hash = blake3::Hash::from_hex(
        std::env::args()
            .nth(1)
            .context("pass the blob's BLAKE3 hash")?,
    )?;

    let buyer = Buyer::connect(Env::from_env()?).await?;
    let (holders, total_bytes) = buyer.holders(*hash.as_bytes()).await?;
    let (candidates, lanes) = buyer.lanes(holders).await?;

    // The streamer keeps its verified range store in a scratch directory. It is
    // not a resumable download: the directory goes when the stream ends.
    let scratch = tempfile::tempdir()?;
    let streamer = Streamer::new(StaticSources::new(candidates)?, NoTopUp, scratch.path());
    // A terminal waits until Ctrl-C; a script gives up after ten minutes
    // without a verified byte.
    let stop = StopPolicy::new(
        std::io::IsTerminal::is_terminal(&std::io::stderr()),
        None,
        Arc::new(ProgressClock::new()),
    );
    let (mut reader, mut drive) = streamer
        .open(
            *hash.as_bytes(),
            total_bytes,
            &PullConfig::new(),
            Arc::new(NoCache),
            stop,
        )
        .await?;

    // Run the drive beside the copy, not inside its reads: a blocked stdout must
    // not stop an open paid leg from paying and draining.
    let mut stdout = tokio::io::stdout();
    let copied = drive
        .alongside(tokio::io::copy(&mut reader, &mut stdout))
        .await;

    buyer.record_payments(&lanes);
    copied?;
    Ok(())
}
