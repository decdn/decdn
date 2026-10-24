//! Arguments for the `decdn gateway` subcommand.

use std::net::SocketAddr;

use clap::Args;
use url::Url;

use super::ClientFetchArgs;
use super::byte_size::parse_byte_size;

/// Serve `decdn:` URIs over HTTP: to a browser from a loopback gateway, or to
/// anyone from a public one.
#[derive(Args, Debug, Clone)]
pub struct GatewayArgs {
    /// Address to listen on: a loopback address, or any address with
    /// `--public-url`. `GET /<hash>[/<path>][?ns=<id>]` serves
    /// `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8484")]
    pub listen: SocketAddr,

    /// The public gateway's origin and allowance; unset for a loopback
    /// gateway.
    #[command(flatten)]
    pub public: PublicArgs,

    /// Discovery and chain flags, as `decdn get` takes them. A loopback
    /// gateway holds its own delegate keys and registered capabilities, so
    /// the keystore and capability flags do not apply to it; a public gateway
    /// pays from `--keystore`'s own pool. Chain coordinates that neither a
    /// flag nor the config file sets default to the built-in testnet, and
    /// `--data-dir` (where the gateway token, or a public gateway's buyer
    /// store, lives) defaults to `~/.decdn/gateway`.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}

/// A public gateway: the origin it serves and the allowance it serves within.
#[derive(Args, Debug, Clone)]
pub struct PublicArgs {
    /// Serve the public internet at this origin, `https://decdn.link`: any
    /// `--listen` address, no token, no capabilities. Every request pays
    /// from the keystore's own pool (`--keystore`,
    /// `--working-deposit-micro-usdc`), within the daily allowance. A bundle
    /// is a site at `[<ns>.]<hash32>.<host>`; register the host on the Public
    /// Suffix List so the sites' cookies stay apart.
    #[arg(long, value_name = "URL")]
    pub public_url: Option<Url>,

    /// Bytes the gateway delivers per UTC day from its pool, over every
    /// request: `1GiB`, `512MiB`, or a count of bytes. A request past it, or
    /// one whose size does not fit what is left, gets 402 with the `decdn:`
    /// URI to fetch with a paying client.
    #[arg(
        long,
        value_name = "BYTES",
        default_value = "1GiB",
        value_parser = parse_byte_size,
        requires = "public_url"
    )]
    pub allowance_bytes_per_day: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests;
