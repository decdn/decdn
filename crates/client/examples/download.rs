//! Download one blob to a file with the [`Downloader`] face.
//!
//! ```text
//! cargo run -p decdn-client --example download -- <blake3-hash-hex> <output-path>
//! ```
//!
//! Set the `DECDN_*` variables that `common::Env::from_env` lists first. The
//! download stripes across every verified holder, checks every byte against the
//! hash, and fails over between holders. A download that stops keeps its
//! `.partial` file beside the output, and a rerun fetches only the missing
//! ranges.

mod common;

use std::path::PathBuf;

use alloy::primitives::U256;
use anyhow::{Context, Result};
use decdn_client::driver::DriveConfig;
use decdn_client::{DownloadTarget, Downloader, PullConfig};

use common::{Buyer, Env, NoTopUp};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let mut args = std::env::args().skip(1);
    let hash = blake3::Hash::from_hex(args.next().context("pass the blob's BLAKE3 hash")?)?;
    let dest = PathBuf::from(args.next().context("pass an output path")?);

    let buyer = Buyer::connect(Env::from_env()?).await?;
    let (holders, total_bytes) = buyer.holders(*hash.as_bytes()).await?;
    let (candidates, lanes) = buyer.lanes(holders).await?;

    // `U256::ZERO` as the working deposit turns reactive top-up off, which
    // matches the `NoTopUp` funder.
    let downloader = Downloader::new(candidates, NoTopUp, DriveConfig::cli(U256::ZERO));
    let result = downloader
        .fetch_to_paths(
            &[DownloadTarget {
                hash: *hash.as_bytes(),
                total_bytes,
                dest: &dest,
            }],
            &PullConfig::new(),
            None,
            None,
        )
        .await;

    // Record the payments before the result: the bytes are paid for either way.
    buyer.record_payments(&lanes);
    result.map(|_| ())
}
