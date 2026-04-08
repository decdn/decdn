//! deCDN node — CLI entry point.

mod cli;
mod config;

use clap::Parser;

use cli::{Cli, Command, LogFormat};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run(run_args) => cmd_run(cli.config.as_deref(), &run_args),
        Command::KeyGen(args) => cmd_key_gen(&args),
        Command::Config(args) => cmd_config_init(&args),
    }
}

/// Run the deCDN node with resolved configuration.
fn cmd_run(config_path: Option<&std::path::Path>, run_args: &cli::RunArgs) -> anyhow::Result<()> {
    let resolved = config::resolve_config(config_path, run_args)?;

    // Initialize tracing — RUST_LOG takes precedence over resolved log level.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(resolved.log_level.to_string()));

    match resolved.log_format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }

    tracing::info!("deCDN node starting");
    tracing::debug!(
        data_dir = %resolved.data_dir.display(),
        bind_port = resolved.bind_port,
        rpc_url = %resolved.rpc_url,
        cache_dir = %resolved.cache_dir.display(),
        cache_size_mb = resolved.cache_size_mb,
        rate_per_mb = resolved.rate_per_mb,
        metrics_port = resolved.metrics_port,
        "resolved configuration"
    );

    Ok(())
}

/// Generate Ed25519 node key and Ethereum keystore.
fn cmd_key_gen(args: &cli::KeyGenArgs) -> anyhow::Result<()> {
    let output_dir = args
        .output_dir
        .clone()
        .or_else(cli::default_data_dir)
        .ok_or_else(|| anyhow::anyhow!("cannot determine output directory: home dir not found"))?;

    // TODO: implement key generation
    println!("would generate keys in {}", output_dir.display());
    if args.force {
        println!("(overwrite mode enabled)");
    }
    Ok(())
}

/// Write a default TOML configuration file.
fn cmd_config_init(args: &cli::ConfigInitArgs) -> anyhow::Result<()> {
    let output = args
        .output
        .clone()
        .or_else(cli::default_config_path)
        .ok_or_else(|| anyhow::anyhow!("cannot determine config path: home dir not found"))?;

    if output.exists() && !args.force {
        anyhow::bail!(
            "config file already exists at {}; use --force to overwrite",
            output.display()
        );
    }

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("failed to create directory {}: {e}", parent.display()))?;
    }

    std::fs::write(&output, DEFAULT_CONFIG)
        .map_err(|e| anyhow::anyhow!("failed to write config file {}: {e}", output.display()))?;

    println!("wrote default config to {}", output.display());
    Ok(())
}

/// Default TOML config file content.
const DEFAULT_CONFIG: &str = r#"# deCDN node configuration
# CLI flags override values in this file.

[identity]
# data_dir = "~/.decdn"
# region = "US"

[network]
# bind_port = 4433
# relay_url = "https://relay.iroh.network."

[blockchain]
# rpc_url = ""                       # REQUIRED: Arbitrum Sepolia JSON-RPC URL
# eth_keystore = "~/.decdn/keystore.json"
# payment_channel_address = ""       # REQUIRED: 0x-prefixed hex
# staking_registry_address = ""      # REQUIRED: 0x-prefixed hex

[cache]
# cache_dir = "~/.decdn/cache"
# cache_size_mb = 10240
# max_blob_size_mb = 10240

[payment]
# rate_per_mb = 10

[observability]
# log_level = "info"
# log_format = "pretty"
# metrics_port = 9090
"#;
