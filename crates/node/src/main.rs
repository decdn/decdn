//! deCDN node — CLI entry point.

use clap::Parser;

/// A deCDN node: cache and serve content-addressed blobs over iroh QUIC.
#[derive(Parser, Debug)]
#[command(name = "decdn", version, about)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    tracing::info!("deCDN node starting");

    Ok(())
}
