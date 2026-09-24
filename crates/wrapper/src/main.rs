//! `decdn-sponsored`: download a content-addressed bundle through the
//! sponsord gateway, with no wallet. See `flow::pull`.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use decdn_sponsored::config::WrapperConfig;
use decdn_sponsored::flow;

/// Download from deCDN, paid for by the sponsor. You solve one captcha per
/// download; there is no wallet, key, or password to manage.
#[derive(Parser, Debug)]
#[command(name = "decdn-sponsored")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Pull a bundle by its BLAKE3 hash.
    Pull {
        /// BLAKE3 hash of the bundle manifest (`b3:<hex>` or bare hex).
        hash: String,

        /// Directory the bundle's files are written under.
        #[arg(short, long, default_value = ".")]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let cfg = match WrapperConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("decdn-sponsored: failed to load config: {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = match &cli.command {
        Command::Pull { hash, output } => flow::pull(hash, output, &cfg).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("decdn-sponsored: {e:#}");
            ExitCode::FAILURE
        }
    }
}
