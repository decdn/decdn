//! CLI argument parsing for the `decdn` binary.
//!
//! [`RunArgs`] is also re-used (flattened) by [`ConfigValidateArgs`] —
//! `decdn config validate` runs the same resolver `decdn-node run`
//! does, against the same `DECDN_*` env vars, without binding ports
//! or starting any tasks. The daemon's own clap shell lives in
//! `crates/node/src/main.rs` and is not part of this module.

pub mod appeal;
pub mod bundle;
pub mod channel;
pub mod common;
pub mod config_cmd;
pub mod fetch;
pub mod key_gen;
pub mod node;
pub mod probe;
pub mod publish;
pub mod run;
pub mod setup;

pub use appeal::{AppealArgs, AppealCommand, AppealSlashArgs};
pub use bundle::{BundleArgs, BundleCommand, BundleCreateArgs, BundlePullArgs};
pub use channel::{
    ChannelArgs, ChannelChainArgs, ChannelCleanArgs, ChannelCloseArgs, ChannelCommand,
    ChannelListArgs, ChannelOpenArgs, ChannelSettleArgs, CoopCloseArgs,
};
pub use common::{
    CommonChainArgs, ConfigPathSource, LogFormat, default_client_data_dir, default_config_path,
    default_data_dir,
};
pub use config_cmd::{ConfigArgs, ConfigCommand, ConfigInitArgs, ConfigValidateArgs};
pub use fetch::{ClientFetchArgs, FetchArgs};
pub use key_gen::KeyGenArgs;
pub use node::{
    AnnounceArgs, BondArgs, ChainArgs, ChannelsArgs, DeregisterArgs, DrainArgs, EvictArgs,
    HealthArgs, LookupArgs, NodeArgs, NodeCommand, PeersArgs, RegionStatsArgs, RegisterArgs,
    ReloadArgs, StatusArgs, SwapVenueArg, TopArgs, UnbondArgs,
};
pub use probe::ProbeArgs;
pub use publish::{
    AssignArgs, NamespaceArgs, NamespaceCommand, NamespaceCreateArgs, PublishArgs,
    PublishChainArgs, PublishCommand, RevokeArgs,
};
pub use run::RunArgs;
pub use setup::{PayBondWith, SetupArgs};

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// User-facing deCDN CLI. The daemon lives in the separate `decdn-node`
/// binary; this CLI is what you type from a terminal — probe, node admin,
/// key-gen, config.
#[derive(Parser, Debug)]
#[command(
    name = "decdn",
    version,
    about = "deCDN user CLI — probe, node admin, key-gen, config",
    long_about = "User-facing deCDN CLI. Pairs with the `decdn-node` daemon: \
                  this binary carries every command a human types in a \
                  terminal (probe, node admin, key-gen, config). The \
                  `node` subcommand group talks to a running daemon over \
                  the loopback admin RPC surface (ADR 025) and fails \
                  cleanly on a host with no daemon running."
)]
pub struct Cli {
    /// Path to TOML config file [default: ~/.decdn/node.toml].
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Available subcommands. There is intentionally no `run` subcommand —
/// running the daemon is `decdn-node run` (issue #421). For a dry-run
/// resolution of the daemon's startup config without binding ports,
/// see [`ConfigValidateArgs`] (`decdn config validate`), which
/// flattens the same [`RunArgs`] the daemon parses.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Generate a new Ed25519 node key and Ethereum keystore.
    KeyGen(KeyGenArgs),
    /// Manage configuration files (`init`, `validate`).
    Config(ConfigArgs),
    /// Probe a running node over the `cdn/probe/v1` ALPN.
    Probe(ProbeArgs),
    /// Fetch a single content-addressed blob from a node over the paid
    /// `cdn/client/v1` path, paying per-MB from an open `PaymentChannel`.
    Fetch(FetchArgs),
    /// Guided node onboarding (ADR 019 Phases 1–2): pre-flight checks,
    /// key generation, bond, and on-chain registration, with a final
    /// readiness summary. Thin orchestration over `key-gen` / `node bond` /
    /// `node register` — submits no transaction the primitives don't.
    Setup(SetupArgs),
    /// Operator-local admin commands that query a running node
    /// over its loopback HTTP surface (ADR 025).
    Node(NodeArgs),
    /// Build and (eventually) fetch directory bundles — a JSON manifest
    /// linking BLAKE3-content-addressed blobs by relative path. See
    /// `appendix-bundles.md` for the format and issue #391 for status.
    Bundle(BundleArgs),
    /// Client-side payment-channel lifecycle (`list`/`status`, `coop-close`).
    Channel(ChannelArgs),
    /// Publisher control plane: create namespaces, request publisher vetting,
    /// and seat or unseat authorized origins on-chain (issues #1029 / #1491).
    /// Content is bound to a namespace off-chain at fetch time, so there is no
    /// per-hash on-chain claim.
    Publish(PublishArgs),
    /// File a slash appeal, posting the appeal bond (ADR 028).
    Appeal(AppealArgs),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Clap builds subcommands lazily along the parsed path, so a malformed arg
    /// definition (duplicate long flag, duplicate arg id) panics at *runtime* on
    /// the first invocation of that subcommand rather than failing the build.
    /// `debug_assert` walks the whole command tree eagerly, so it catches this
    /// for every subcommand — including the ones no test parses.
    ///
    /// This matters most for the flattened [`CommonChainArgs`]: it is embedded in
    /// both `ChainArgs` and `PublishChainArgs`, so a future struct that flattens
    /// two of them (or re-declares a shared flag by hand) would collide.
    #[test]
    fn cli_command_tree_is_clap_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
