//! Shared relay resolution for the one-shot client commands (`fetch`, `probe`).
//!
//! Relays are environment configuration, not a per-invocation decision (#935):
//! they come from the config file (`network.relay_urls`), exactly the field the
//! node already consumes. `--relay-url` stays as an optional override that replaces the
//! config list. An absent config file yields no relays, so a client that dials
//! a direct `--addr` needs no config at all.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::str::FromStr;

use decdn_common::config::{ResolvedDiscovery, load_file_config, resolve_discovery};
use decdn_common::identity::fresh_secret_key;
use decdn_common::redact::redact_userinfo;
use iroh::address_lookup::{DnsAddressLookup, MemoryLookup};
use iroh::endpoint::{QuicTransportConfig, VarInt, presets};
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMap, RelayMode, RelayUrl};

/// Per-stream QUIC receive window for a client pull.
///
/// QUIC flow control is receiver-advertised, and a download makes the client
/// the receiver, so this window — not any node-side setting — bounds
/// single-stream throughput at roughly `window / RTT`. It matches the paid
/// path's own ceiling: a node serves at most one credit window of unpaid bytes
/// ahead of the paid frontier (`clamp(paid/divisor, 4 MiB, 64 MiB)`, ADR 003),
/// so unpaid in-flight never exceeds 64 MiB regardless of transport buffering.
/// Sizing this to that same 64 MiB makes the transport window match the payment
/// ceiling — below the credit cap the transport is never the bottleneck at any
/// RTT, and above it a larger window would buy nothing because the credit
/// window blocks first.
const CLIENT_STREAM_RECEIVE_WINDOW: u32 = 64 * 1024 * 1024;

/// Whole-connection QUIC receive window for a client pull. A pull runs one bulk
/// stream per connection (`per_source_inflight = 1`, one connection per
/// provider), so this only needs to hold at least
/// [`CLIENT_STREAM_RECEIVE_WINDOW`]; the 2x headroom leaves slack for the
/// connection's control traffic without letting worst-case buffering grow
/// larger than it must.
const CLIENT_RECEIVE_WINDOW: u32 = 128 * 1024 * 1024;

/// QUIC transport config for a one-shot client [`Endpoint`]. It raises only the
/// receive windows over the iroh defaults: on a download the client is the
/// flow-control receiver, so these windows bound single-stream throughput on a
/// high-latency path. Congestion control lives at the sender, so a node-side
/// change is what would alter that half.
fn client_transport_config() -> QuicTransportConfig {
    QuicTransportConfig::builder()
        .stream_receive_window(VarInt::from_u32(CLIENT_STREAM_RECEIVE_WINDOW))
        .receive_window(VarInt::from_u32(CLIENT_RECEIVE_WINDOW))
        .build()
}

/// Resolve the relay URLs for a client command. The `--relay-url` override
/// (`flag`) wins; otherwise `network.relay_urls` from the config is used. An
/// absent config file
/// — including an explicit `--config` path that does not exist — resolves to
/// an empty list rather than an error, so a client dialing a direct `--addr`
/// needs no config at all.
pub fn resolve_relays(
    flag: Option<&str>,
    config_path: Option<&Path>,
) -> anyhow::Result<Vec<RelayUrl>> {
    let raw: Vec<String> = if let Some(s) = flag {
        vec![s.to_owned()]
    } else if config_path.is_none_or(Path::exists) {
        // `None` → `load_file_config` resolves the default path (and returns
        // an empty default when it is absent). An explicit path is only loaded
        // when it exists; a present-but-malformed file still surfaces its parse
        // error.
        let net = load_file_config(config_path)?.network.unwrap_or_default();
        net.relay_urls.unwrap_or_default()
    } else {
        Vec::new()
    };
    raw.iter()
        .map(|s| {
            // Redact any `user:pass@` userinfo before echoing a malformed entry
            // into an error that may reach logs (mirrors node bring-up's
            // `parse_relay_urls`).
            RelayUrl::from_str(s)
                .map_err(|e| anyhow::anyhow!("invalid relay url {:?}: {e}", redact_userinfo(s)))
        })
        .collect()
}

/// Build the `RelayMode` for a one-shot client: a custom map when any relays
/// are configured, else disabled (the client dials a direct `--addr`). Mirrors
/// the client commands' prior behaviour, just sourced from the resolved list.
#[must_use]
pub fn relay_mode(relays: &[RelayUrl]) -> RelayMode {
    if relays.is_empty() {
        RelayMode::Disabled
    } else {
        RelayMode::Custom(relays.iter().cloned().collect::<RelayMap>())
    }
}

/// Resolve `[network.discovery]` for a client command, tolerating a missing
/// config exactly like [`resolve_relays`]: an explicit `--config` path that does
/// not exist yields no discovery overrides (→ `presets::N0`), so a client that
/// dials a direct `--addr` needs no config at all.
///
/// # Errors
///
/// Surfaces a present-but-malformed config's parse/validation error.
pub fn client_discovery(config_path: Option<&Path>) -> anyhow::Result<ResolvedDiscovery> {
    if config_path.is_some_and(|p| !p.exists()) {
        return Ok(ResolvedDiscovery::default());
    }
    resolve_discovery(&load_file_config(config_path)?)
}

/// Build a one-shot client [`Endpoint`] that can resolve a target node by its
/// iroh `NodeId`. With no `[network.discovery]` config we use `presets::N0`
/// (the n0-hosted pkarr/DNS lookup the node defaults to); with operator
/// discovery configured we compose only the *resolution* legs onto
/// `presets::Minimal`.
///
/// Relay selection is an independent leg, mirroring the node's `build_endpoint`:
/// a configured relay list (`--relay-url`/`network.relay_urls`, #935) becomes a
/// `RelayMode::Custom` map; with no custom relays we keep `presets::N0`'s n0
/// default relay map, and on `presets::Minimal` (operator discovery) we restore
/// `RelayMode::Default` — dropping the n0 *discovery* leg must not also disable
/// relays, or a NodeId-only dial of a NAT'd node could not connect.
///
/// Note: the pkarr *publisher* leg the node wires (`network.discovery.pkarr_url`)
/// is intentionally omitted — a one-shot client resolves peers, it never
/// publishes its own ephemeral address record. Resolution for an operator
/// namespace goes through its `dns_origin`, which `resolve_config` already
/// requires whenever `pkarr_url` is set. Upgrade path: add a pkarr resolver leg
/// here if a deployment ever resolves via pkarr without a DNS bridge.
pub async fn client_endpoint(
    relays: &[RelayUrl],
    discovery: &ResolvedDiscovery,
) -> anyhow::Result<Endpoint> {
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0);
    let mut builder = if discovery.is_empty() {
        Endpoint::builder(presets::N0)
    } else {
        add_resolution_lookups(Endpoint::builder(presets::Minimal), discovery)?
    };
    builder = builder
        .secret_key(fresh_secret_key())
        .transport_config(client_transport_config());
    builder = if !relays.is_empty() {
        builder.relay_mode(relay_mode(relays))
    } else if discovery.is_empty() {
        builder // presets::N0 already carries the n0 default relay map.
    } else {
        // presets::Minimal sets no relay mode; restore the n0 default.
        builder.relay_mode(RelayMode::Default)
    };
    builder
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))
}

/// Compose the operator-configured *resolution* legs onto `builder`: a
/// `DnsAddressLookup` for `dns_origin` and a `MemoryLookup` for any static
/// peers. Mirrors the node's `add_discovery_lookups` minus the publisher leg
/// (see [`client_endpoint`]). The peer fields were shape-validated at
/// resolution; they are re-parsed here into iroh types, and any echoed URL is
/// `redact_userinfo`'d so a credential-bearing typo never reaches an error.
fn add_resolution_lookups(
    mut builder: iroh::endpoint::Builder,
    discovery: &ResolvedDiscovery,
) -> anyhow::Result<iroh::endpoint::Builder> {
    if let Some(origin) = &discovery.dns_origin {
        builder = builder.address_lookup(DnsAddressLookup::builder(origin.clone()));
    }
    if !discovery.peers.is_empty() {
        let mut infos = Vec::with_capacity(discovery.peers.len());
        for peer in &discovery.peers {
            let id = peer.node_id.parse::<PublicKey>().map_err(|e| {
                anyhow::anyhow!("invalid network.discovery peer id {}: {e}", peer.node_id)
            })?;
            let mut addr = EndpointAddr::new(id);
            if let Some(relay) = &peer.relay_url {
                let relay_url = relay.parse::<RelayUrl>().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid relay_url {:?} for network.discovery peer {}: {e}",
                        redact_userinfo(relay),
                        peer.node_id
                    )
                })?;
                addr = addr.with_relay_url(relay_url);
            }
            for a in &peer.addrs {
                let sock = a.parse::<std::net::SocketAddr>().map_err(|e| {
                    anyhow::anyhow!(
                        "invalid addr {a:?} for network.discovery peer {}: {e}",
                        peer.node_id
                    )
                })?;
                addr = addr.with_ip_addr(sock);
            }
            infos.push(addr);
        }
        builder = builder.address_lookup(MemoryLookup::from_endpoint_info(infos));
    }
    Ok(builder)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_config(body: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn flag_overrides_config() {
        // Config has a relay, but the flag wins and the config is not consulted.
        let cfg = write_config("[network]\nrelay_urls = [\"https://cfg.example.com\"]\n");
        let relays = resolve_relays(Some("https://flag.example.com"), Some(cfg.path())).unwrap();
        assert_eq!(relays.len(), 1);
        assert!(relays[0].to_string().contains("flag.example.com"));
    }

    #[test]
    fn config_relay_urls_are_read_when_no_flag() {
        let cfg = write_config(
            "[network]\nrelay_urls = [\"https://a.example.com\", \"https://b.example.com\"]\n",
        );
        let relays = resolve_relays(None, Some(cfg.path())).unwrap();
        assert_eq!(relays.len(), 2);
    }

    #[test]
    fn config_without_network_section_yields_empty() {
        let cfg = write_config("");
        let relays = resolve_relays(None, Some(cfg.path())).unwrap();
        assert!(relays.is_empty());
    }

    #[test]
    fn missing_explicit_config_path_yields_empty_not_error() {
        // A `--config` path that does not exist must not fail relay resolution:
        // a client dialing a direct `--addr` supplies no relays at all.
        let missing = Path::new("/nonexistent/decdn-relay-test-does-not-exist.toml");
        let relays = resolve_relays(None, Some(missing)).expect("missing config must not error");
        assert!(relays.is_empty());
    }

    #[test]
    fn malformed_relay_error_redacts_userinfo() {
        // A malformed entry carrying credentials must not leak them in the error.
        let err = resolve_relays(Some("http://user:s3cret@ relay"), None)
            .expect_err("malformed relay url must error");
        let msg = err.to_string();
        assert!(!msg.contains("s3cret"), "password must be redacted: {msg}");
    }

    #[test]
    fn empty_relays_disable_relay_mode() {
        assert!(matches!(relay_mode(&[]), RelayMode::Disabled));
        let one = vec![RelayUrl::from_str("https://relay.example.com").unwrap()];
        assert!(matches!(relay_mode(&one), RelayMode::Custom(_)));
    }
}
