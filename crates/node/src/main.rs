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

    let default = if cli.verbose { "debug" } else { "info" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    tracing::info!("deCDN node starting");

    Ok(())
}
