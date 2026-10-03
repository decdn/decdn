//! `decdn-sponsored`: download a content-addressed bundle through the
//! sponsord onramp, with no wallet. See `pull::pull`.

use std::num::NonZeroU64;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use decdn_sponsored::config::Config;
use decdn_sponsored::pull;

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

        /// Namespace the bundle is published under. Lets a node that has not
        /// cached it pull from that namespace's origins; without it the bundle
        /// is served from caches only. Never changes which bytes you get: they
        /// are verified against the hash either way.
        #[arg(long, value_name = "ID")]
        namespace: Option<NonZeroU64>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let cfg = match Config::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("decdn-sponsored: failed to load config: {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = match &cli.command {
        Command::Pull {
            hash,
            output,
            namespace,
        } => pull::pull(hash, output, *namespace, &cfg).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("decdn-sponsored: {e:#}");
            ExitCode::FAILURE
        }
    }
}
