//! CLI argument parsing for the `decdn` binary.
//!
//! [`RunArgs`] is also re-used (flattened) by [`ConfigValidateArgs`] —
//! `decdn config validate` runs the same resolver `decdn-node run`
//! does, against the same `DECDN_*` env vars, without binding ports
//! or starting any tasks. The daemon's own clap shell lives in
//! `crates/node/src/main.rs` and is not part of this module.

pub mod appeal;
pub mod byte_size;
pub mod common;
pub mod config_cmd;
pub mod dashboard;
pub mod fetch;
pub mod gateway;
pub mod get;
pub mod key_gen;
pub mod node;
pub mod origin;
pub mod pool;
pub mod probe;
pub mod publish;
pub mod quote;
pub mod receipt;
pub mod run;
pub mod self_update;
pub mod setup;
pub mod whoami;

pub use appeal::{AppealArgs, AppealCommand, AppealSlashArgs};
pub use byte_size::parse_byte_size;
pub use common::{
    CommonChainArgs, ConfigPathSource, LogFormat, default_client_data_dir, default_config_path,
    default_data_dir,
};
pub use config_cmd::{ConfigArgs, ConfigCommand, ConfigInitArgs, ConfigValidateArgs};
pub use dashboard::DashboardArgs;
pub use fetch::ClientFetchArgs;
pub use gateway::GatewayArgs;
pub use get::GetArgs;
pub use key_gen::KeyGenArgs;
pub use node::{
    BondArgs, ChainArgs, DeregisterArgs, DoctorArgs, DrainArgs, EarningsArgs, EvictArgs,
    HealthArgs, LanesArgs, LookupArgs, NodeArgs, NodeCommand, PoolsArgs, RegisterArgs, ReloadArgs,
    RotateKeyArgs, RotateKeyTarget, SlashesArgs, StatusArgs, TopArgs, UnbondArgs,
    UpdateMultiaddrsArgs, UpdateRegionArgs,
};
pub use origin::{OriginArgs, OriginCommand, OriginImportArgs};
pub use pool::{
    PoolArgs, PoolAssignArgs, PoolChainArgs, PoolCloseArgs, PoolCommand, PoolListArgs,
    PoolOpenArgs, PoolReclaimArgs, PoolTopUpArgs,
};
pub use probe::ProbeArgs;
pub use publish::{
    AssignArgs, NamespaceArgs, NamespaceCommand, NamespaceCreateArgs, PointArgs, PublishArgs,
    PublishChainArgs, PublishCommand, RevokeArgs,
};
pub use quote::QuoteArgs;
pub use receipt::{ReceiptArgs, ReceiptCommand, ReceiptSettleArgs, ReceiptVerifyArgs};
pub use run::RunArgs;
pub use self_update::SelfUpdateArgs;
pub use setup::SetupArgs;
pub use whoami::WhoamiArgs;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// User-facing deCDN CLI. The daemon lives in the separate `decdn-node`
/// binary; this CLI is what you type from a terminal — probe, get, pools,
/// publishing, node admin.
#[derive(Parser, Debug)]
#[command(
    name = "decdn",
    version,
    about = "deCDN user CLI — probe, get, pools, publishing, node admin",
    long_about = "User-facing deCDN CLI. Pairs with the `decdn-node` daemon: \
                  this binary carries every command a human types in a \
                  terminal (probe, get, payment pools, publishing, \
                  origin import, node admin, key-gen, config). The \
                  `node` subcommand group talks to a running daemon over \
                  the loopback admin RPC surface \
                  (`appendix-local-admin-http.md`) and fails cleanly on a \
                  host with no daemon running."
)]
pub struct Cli {
    /// Path to TOML config file [default: ~/.decdn/node.toml].
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Enable client-side logging at this level. Off by default (the CLI is a
    /// terminal UI); `RUST_LOG` overrides this when set. Useful for diagnosing
    /// a stalled `decdn get` from the client side. Deliberately not bound to
    /// an environment variable: the CLI stays silent unless logging is asked
    /// for on this invocation, even when the daemon's log-level env is exported.
    #[arg(long, global = true, value_name = "LEVEL")]
    pub log_level: Option<common::LogLevel>,

    /// Enable client-side logging by count: `-v` is `info`, `-vv` is `debug`,
    /// `-vvv` is `trace`. Shorthand for `--log-level`, which wins when both are
    /// given; `RUST_LOG` wins over both.
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

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
    /// Print the local identity read-only: iroh node id, eth address, and the
    /// resolved key paths. Generates and rotates nothing.
    Whoami(WhoamiArgs),
    /// Manage configuration files (`init`, `validate`).
    Config(ConfigArgs),
    /// Probe a running node over the `cdn/probe/v1` ALPN.
    Probe(ProbeArgs),
    /// Serve `decdn:` URIs over HTTP: to a browser from a loopback gateway,
    /// paid with the scoped capabilities pages and the token holder register,
    /// or to anyone from a public gateway (`--public-url`) paid from its own
    /// pool within a daily allowance.
    Gateway(Box<GatewayArgs>),
    /// Serve one page, on a loopback address, of this host's `decdn get`
    /// pulls, its gateway when one answers, and its node when one runs.
    Dashboard(Box<DashboardArgs>),
    /// Pull what a `decdn:` URI names: a blob (`decdn:<hash>`), a whole bundle
    /// (`decdn:<hash>/`), or one bundle entry (`decdn:<hash>/<path>`), over the
    /// paid `cdn/client/v1` path, paying per-MB from a `PaymentPool`.
    Get(Box<GetArgs>),
    /// Price a pull of what a `decdn:` URI names from the rates the nodes
    /// sign in their probe answers, before any paid byte.
    Quote(Box<QuoteArgs>),
    /// Check a pull receipt `decdn get --receipt` wrote (`verify`), or add the
    /// on-chain redemptions that settle it (`settle`).
    Receipt(ReceiptArgs),
    /// Guided node onboarding (ADR 019 Phases 1–2): pre-flight checks,
    /// key generation, bond, and on-chain registration, with a final
    /// readiness summary. Thin orchestration over `key-gen` / `node bond` /
    /// `node register` — submits no transaction the primitives don't.
    Setup(SetupArgs),
    /// Operator-local admin commands that query a running node
    /// over its loopback HTTP surface (`appendix-local-admin-http.md`).
    Node(NodeArgs),
    /// Seed a cache origin store from local content (`import`) — offline,
    /// config-free filesystem work that writes each blob's sharded data object
    /// and its `{hex}.obao4` outboard in the layout the daemon reads (ADR 037).
    Origin(OriginArgs),
    /// Client-side payment-pool lifecycle (`list`/`status`, `open`, `top-up`,
    /// `close`, `reclaim`, `assign`).
    Pool(PoolArgs),
    /// Publisher control plane: create namespaces, and seat or unseat
    /// authorized origins on-chain (issues #1029 / #1491).
    /// Content is bound to a namespace off-chain at fetch time, so there is no
    /// per-hash on-chain claim.
    Publish(PublishArgs),
    /// File a slash appeal, posting the appeal bond (ADR 028).
    Appeal(AppealArgs),
    /// Replace this binary with the latest signed release. The release's
    /// `SHA256SUMS.minisig` must verify against a minisign key this build
    /// embeds; a binary in a cargo bin directory (`cargo install`,
    /// `cargo binstall`), or one in a directory this user cannot write, is
    /// refused with the command to run instead.
    SelfUpdate(SelfUpdateArgs),
}

/// Whether the parsed command asked for JSON output, at whatever depth its
/// `--json` flag sits. A command with no `--json` flag never asks.
#[must_use]
pub fn json_requested(matches: &clap::ArgMatches) -> bool {
    matches!(matches.try_get_one::<bool>("json"), Ok(Some(true)))
        || matches
            .subcommand()
            .is_some_and(|(_, sub)| json_requested(sub))
}

#[cfg(test)]
mod tests;
