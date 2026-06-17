//! Arguments for the `decdn probe` subcommand.

use std::net::SocketAddr;

use clap::Args;

/// Probe a running deCDN node over the `cdn/probe/v1` ALPN.
///
/// Connects to the target node, sends a [`ProbeRequest`](decdn_protocol::message::ProbeRequest),
/// and prints the [`ProbeResponse`](decdn_protocol::message::ProbeResponse). Reachability (a
/// direct `--addr` or a relay) is validated at runtime rather than by a clap `ArgGroup`,
/// because the relay can also come from config (`network.relay_urls`, #935).
#[derive(Args, Debug)]
pub struct ProbeArgs {
    /// Target node id (iroh `EndpointId`, z-base32).
    #[arg(long, value_name = "ID")]
    pub node_id: String,

    /// BLAKE3 hash of the blob to query availability for: 64 hex chars
    /// (optional `0x` prefix), the same form `cache.pinned_hashes` accepts.
    /// `cdn/probe/v1` is a content-availability query — the node answers
    /// whether it holds this blob (ADR 005).
    #[arg(long, value_name = "HASH")]
    pub hash: String,

    /// Direct socket address of the target node (e.g. `127.0.0.1:4433`).
    #[arg(long, value_name = "HOST:PORT")]
    pub addr: Option<SocketAddr>,

    /// iroh relay URL to use for discovery-based resolution. Overrides
    /// `network.relay_urls` from config when set (#935); omit it to use the
    /// configured relays.
    #[arg(long, value_name = "URL")]
    pub relay_url: Option<String>,

    /// Overall timeout for the probe roundtrip, in milliseconds.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub timeout_ms: u64,

    /// Print the response as a single line of JSON instead of a pretty block.
    #[arg(long)]
    pub json: bool,
}
