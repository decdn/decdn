//! Arguments for the `decdn config` command group (`init`, `validate`).

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::run::RunArgs;

/// Manage `decdn` configuration files.
#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

/// Subcommands of `decdn config`.
#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Write a default configuration file.
    Init(ConfigInitArgs),
    /// Validate a configuration file without starting the node.
    //
    // Boxed to keep the enum variant size close to `Init`'s — `ConfigValidateArgs`
    // flattens the large `RunArgs` struct. Mirrors `Command::Run(Box<RunArgs>)`.
    Validate(Box<ConfigValidateArgs>),
}

/// Write a default TOML configuration file.
#[derive(Args, Debug)]
pub struct ConfigInitArgs {
    /// Path to write the config file [default: ~/.decdn/node.toml].
    #[arg(long, value_name = "PATH")]
    pub output: Option<PathBuf>,

    /// Overwrite existing config file if present.
    #[arg(long)]
    pub force: bool,

    /// Deployment to bake in: chain id + contract addresses are filled from the
    /// shipped deployment manifest so the config runs out of the box. Omit to
    /// select the only known network; pass `none` for a blank template with
    /// every address left to fill in.
    #[arg(long, value_name = "NAME")]
    pub chain: Option<String>,

    /// Configure an ORIGIN node: write an active `[cache.origin]` pull-through
    /// backend. Accepts an `http(s)://` base URL (http origin), a `file:///`
    /// path (fs origin), or `s3` / `s3://<bucket>` (S3-compatible origin —
    /// bare `s3` leaves a placeholder bucket to edit; either form leaves
    /// region/endpoint/prefix for the generated comments to walk through).
    /// Presence of an origin backend is the role signal —
    /// `cache.relay_foreign_namespaces` then derives to `false` (origin-only).
    /// Omit for a relay node (no origin; serves foreign content for pay;
    /// derives to `true`).
    #[arg(long, value_name = "URL", conflicts_with = "client")]
    pub origin: Option<String>,

    /// Write a fetch-only CLIENT config: identity + chain coordinates for a
    /// consumer that fetches and pays (`decdn fetch`), with none of the
    /// cache/origin/serving sections a `decdn-node` daemon reads.
    #[arg(long)]
    pub client: bool,
}

/// Validate the resolved configuration without binding ports or connecting to
/// the RPC endpoint.
///
/// Flattens the same argument groups as `decdn-node run` so that `DECDN_*`
/// environment variables and optional CLI overrides are honored exactly as
/// they would be during the daemon's startup.
#[derive(Args, Debug)]
pub struct ConfigValidateArgs {
    #[command(flatten)]
    pub run: RunArgs,
}
