//! Arguments for the `decdn probe` subcommand.

use std::net::SocketAddr;

use clap::Args;

/// Probe a running deCDN node over the `cdn/probe/v1` ALPN.
///
/// Connects to the target node, sends a [`ProbeRequest`](decdn_protocol::message::ProbeRequest),
/// and prints the [`ProbeResponse`](decdn_protocol::message::ProbeResponse). At least one of
/// `--addr` or `--relay-url` must be provided.
#[derive(Args, Debug)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .multiple(true)
        .args(["addr", "relay_url"]),
))]
pub struct ProbeArgs {
    /// Target node id (iroh `EndpointId`, z-base32).
    #[arg(long, value_name = "ID")]
    pub node_id: String,

    /// Direct socket address of the target node (e.g. `127.0.0.1:4433`).
    #[arg(long, value_name = "HOST:PORT")]
    pub addr: Option<SocketAddr>,

    /// iroh relay URL to use for discovery-based resolution.
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// Overall timeout for the probe roundtrip, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub timeout_ms: u64,

    /// Print the response as a single line of JSON instead of a pretty block.
    #[arg(long)]
    pub json: bool,
}
