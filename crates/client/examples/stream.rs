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

use alloy::primitives::U256;
use anyhow::{Context, Result};
use decdn_client::driver::DriveConfig;
use decdn_client::{NoCache, PullConfig, Streamer};

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
    let streamer = Streamer::new(
        candidates,
        NoTopUp,
        DriveConfig::cli(U256::ZERO),
        scratch.path(),
    );
    let (mut reader, mut drive) = streamer
        .open(
            *hash.as_bytes(),
            total_bytes,
            &PullConfig::new(),
            Arc::new(NoCache),
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
