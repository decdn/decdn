//! Arguments for the `decdn gateway` subcommand.

use std::net::SocketAddr;

use clap::Args;

use super::ClientFetchArgs;

/// Serve `decdn:` URIs to a browser from a loopback HTTP gateway.
#[derive(Args, Debug, Clone)]
pub struct GatewayArgs {
    /// Loopback address to listen on. `GET /<hash>[/<path>][?ns=<id>]` serves
    /// `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8484")]
    pub listen: SocketAddr,

    /// Discovery and chain flags, as `decdn get` takes them. The gateway
    /// holds its own delegate keys and registered capabilities, so the
    /// keystore and capability flags do not apply. Chain coordinates that
    /// neither a flag nor the config file sets default to the built-in
    /// testnet, and `--data-dir` (where the gateway token lives) defaults to
    /// `~/.decdn/gateway`.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}
