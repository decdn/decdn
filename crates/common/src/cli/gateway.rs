//! Arguments for the `decdn gateway` subcommand.

use std::net::SocketAddr;

use clap::Args;

use super::ClientFetchArgs;

/// Serve `decdn:` URIs to a browser from a loopback HTTP gateway.
#[derive(Args, Debug, Clone)]
pub struct GatewayArgs {
    /// Loopback address to listen on. `GET /<hash>[/<path>][?ns=<id>]` serves
    /// `decdn:<hash>[/<path>][?ns=<id>]`, and `/` shows the allowance.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8484")]
    pub listen: SocketAddr,

    /// The sponsored onramp that funds the gateway's throwaway key. Unused when
    /// `--capability` or `--capability-file` names an allowance the caller
    /// holds.
    #[arg(long, value_name = "URL", default_value = "https://up.decdn.org")]
    pub onramp: String,

    /// Discovery, chain and pool flags, as `decdn fetch` takes them. Chain
    /// coordinates that neither a flag nor the config file sets default to the
    /// built-in testnet, and `--data-dir` defaults to `~/.decdn/gateway`.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
