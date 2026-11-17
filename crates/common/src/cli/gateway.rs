//! Arguments for the `decdn gateway` subcommand.

use std::net::SocketAddr;

use clap::Args;
use url::Url;

use super::ClientFetchArgs;

/// Serve `decdn:` URIs over HTTP: to a browser from a loopback gateway, or to
/// anyone from a public one.
#[derive(Args, Debug, Clone)]
pub struct GatewayArgs {
    /// Address to listen on: a loopback address, or any address with
    /// `--public-url`. `GET /<hash>[/<path>][?ns=<id>]` serves
    /// `decdn:<hash>[/<path>][?ns=<id>]`.
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8484")]
    pub listen: SocketAddr,

    /// The public gateway's origin; unset for a loopback gateway.
    #[command(flatten)]
    pub public: PublicArgs,

    /// Also serve the gateway's metrics (Prometheus text, `GET /metrics`)
    /// and its state (`GET /_status`, which `decdn dashboard --gateway`
    /// shows) on this loopback address, for the operator, with no token.
    /// Without it no such listener runs; the gateway still serves
    /// `/_status` on its own host.
    #[arg(long, value_name = "ADDR", value_parser = parse_loopback_addr)]
    pub metrics_listen: Option<SocketAddr>,

    /// MiB the gateway keeps of what it pulled, under `<data-dir>/cache`, to
    /// serve a later request for the same bytes without a pull. A request
    /// the cache answers whole costs no capability. It shrinks below this to
    /// keep 8 GiB of the volume free; `0` turns the cache off.
    #[arg(long, value_name = "MIB", default_value_t = 10240)]
    pub cache_mb: u64,

    /// Discovery and chain flags, as `decdn get` takes them. The gateway
    /// holds its own delegate keys and pays only with the capabilities
    /// registered to them (`PUT /_capability`), so the keystore and
    /// capability flags are refused. Chain coordinates that neither a flag
    /// nor the config file sets default to the built-in testnet, and
    /// `--data-dir` (where the cache, and a public gateway's namespace
    /// pointers, live) defaults to `~/.decdn/gateway`.
    #[command(flatten)]
    pub common: ClientFetchArgs,
}

/// A public gateway: the origin it serves.
#[derive(Args, Debug, Clone)]
pub struct PublicArgs {
    /// Serve the public internet at this origin, `https://decdn.link`: any
    /// `--listen` address, no token. Every request pays with the capabilities
    /// registered on the bare host. A bundle is a site at
    /// `[<ns>.]<hash32>.<host>`; register the host on the Public Suffix List
    /// so the sites' cookies stay apart.
    #[arg(long, value_name = "URL")]
    pub public_url: Option<Url>,

    /// `PublisherRegistry` address the gateway verifies namespace pointers
    /// against (`PUT /_pointer`). Defaults to `blockchain.publisher_registry_address`
    /// in the config file, else the built-in testnet's on that chain.
    #[arg(long, value_name = "ADDR", requires = "public_url")]
    pub publisher_registry_address: Option<String>,
}

/// Parse a socket address on a loopback interface: what `--metrics-listen`
/// serves is the operator's, never the public's.
pub(crate) fn parse_loopback_addr(s: &str) -> Result<SocketAddr, String> {
    let addr: SocketAddr = s.parse().map_err(|e| format!("{s:?}: {e}"))?;
    if addr.ip().is_loopback() {
        Ok(addr)
    } else {
        Err(format!(
            "{addr} is not a loopback address: the listener serves the operator, not the public"
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests;
