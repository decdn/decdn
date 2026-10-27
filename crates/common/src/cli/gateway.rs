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

    /// Also serve the gateway's metrics (Prometheus text, `GET /metrics`),
    /// its state (`GET /_status`) and the dashboard (`GET /`) on this
    /// loopback address, for the operator. A public gateway's `/_status`
    /// there takes no token; a loopback gateway's takes the gateway token.
    /// Without it no such listener runs; a loopback gateway still serves
    /// `/_status` and the dashboard on its own host.
    #[arg(long, value_name = "ADDR", value_parser = parse_loopback_addr)]
    pub metrics_listen: Option<SocketAddr>,

    /// Discovery and chain flags, as `decdn get` takes them. A loopback
    /// gateway holds its own delegate keys and registered capabilities, so
    /// the keystore and capability flags do not apply to it; a public gateway
    /// pays from `--keystore`'s own pool. Chain coordinates that neither a
    /// flag nor the config file sets default to the built-in testnet, and
    /// `--data-dir` (where the gateway token, or a public gateway's buyer
    /// store and namespace pointers, live) defaults to `~/.decdn/gateway`.
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

    /// Bytes per UTC day for one namespace, `<ns>=<bytes>` (`7=20GiB`),
    /// repeatable. It replaces the shared allowance for the requests the
    /// namespace's pointer serves, at `<ns>.<host>`, and for those alone: a
    /// `?ns=` or a `<ns>.<hash32>.` label is anyone's to write and spends the
    /// shared allowance.
    #[arg(
        long,
        value_name = "NS=BYTES",
        value_parser = parse_namespace_allowance,
        requires = "public_url"
    )]
    pub allowance: Vec<(u64, u64)>,

    /// `PublisherRegistry` address the gateway verifies namespace pointers
    /// against (`PUT /_pointer`). Defaults to `blockchain.publisher_registry_address`
    /// in the config file, else the built-in testnet's on that chain.
    #[arg(long, value_name = "ADDR", requires = "public_url")]
    pub publisher_registry_address: Option<String>,
}

/// Parse a socket address on a loopback interface: what `--metrics-listen`
/// serves is the operator's, never the public's.
fn parse_loopback_addr(s: &str) -> Result<SocketAddr, String> {
    let addr: SocketAddr = s.parse().map_err(|e| format!("{s:?}: {e}"))?;
    if addr.ip().is_loopback() {
        Ok(addr)
    } else {
        Err(format!(
            "{addr} is not a loopback address: the listener serves the operator, not the public"
        ))
    }
}

/// Parse `<ns>=<bytes>`: a canonical decimal namespace id (no leading zero,
/// never `0`) and a byte size as `--allowance-bytes-per-day` takes it.
fn parse_namespace_allowance(s: &str) -> Result<(u64, u64), String> {
    let Some((ns, bytes)) = s.split_once('=') else {
        return Err(format!("expected <ns>=<bytes>, got {s:?}"));
    };
    if ns.is_empty() || ns.starts_with('0') || !ns.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "invalid namespace id {ns:?}: expected a decimal id of 1 or more with no leading zero"
        ));
    }
    let ns: u64 = ns
        .parse()
        .map_err(|_| format!("namespace id {ns:?} does not fit in 64 bits"))?;
    Ok((ns, parse_byte_size(bytes)?))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests;
