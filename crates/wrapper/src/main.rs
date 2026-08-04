//! `onramp`: fetch a content-addressed blob through the decdn-sponsor
//! gateway. Wraps `decdn fetch` with fund/poll/top-up handling — see
//! `flow::get`.

use std::path::PathBuf;

use clap::Parser;
use onramp::config::WrapperConfig;
use onramp::flow;

/// Fetch a blob through the decdn-sponsor gateway.
#[derive(Parser, Debug)]
#[command(name = "onramp")]
struct Cli {
    /// BLAKE3 hash of the blob to fetch.
    hash: String,

    /// Destination path for the fetched blob.
    #[arg(short, long)]
    output: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let cfg = match WrapperConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("onramp: failed to load config: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = flow::get(&cli.hash, &cli.output, &cfg).await {
        eprintln!("onramp: {e}");
        std::process::exit(1);
    }
}
