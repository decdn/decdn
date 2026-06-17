//! Shared relay resolution for the one-shot client commands (`fetch`, `probe`).
//!
//! Relays are environment configuration, not a per-invocation decision (#935):
//! they come from the config file (`network.relay_urls`, with the deprecated
//! singular `network.relay_url` folded in), exactly the field the node already
//! consumes. `--relay-url` stays as an optional override that replaces the
//! config list. An absent config file yields no relays, so a client that dials
//! a direct `--addr` needs no config at all.

use std::path::Path;
use std::str::FromStr;

use decdn_common::config::load_file_config;
use iroh::{RelayMap, RelayMode, RelayUrl};

/// Resolve the relay URLs for a client command. The `--relay-url` override
/// (`flag`) wins; otherwise `network.relay_urls` from the config (with the
/// deprecated singular `relay_url` folded in) is used. An absent config file
/// resolves to an empty list.
pub fn resolve_relays(
    flag: Option<&str>,
    config_path: Option<&Path>,
) -> anyhow::Result<Vec<RelayUrl>> {
    let raw: Vec<String> = if let Some(s) = flag {
        vec![s.to_owned()]
    } else {
        let net = load_file_config(config_path)?.network.unwrap_or_default();
        match net.relay_urls {
            Some(urls) if !urls.is_empty() => urls,
            _ => net.relay_url.into_iter().collect(),
        }
    };
    raw.iter()
        .map(|s| RelayUrl::from_str(s).map_err(|e| anyhow::anyhow!("invalid relay url {s:?}: {e}")))
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
    fn deprecated_singular_relay_url_is_folded_in() {
        let cfg = write_config("[network]\nrelay_url = \"https://old.example.com\"\n");
        let relays = resolve_relays(None, Some(cfg.path())).unwrap();
        assert_eq!(relays.len(), 1);
        assert!(relays[0].to_string().contains("old.example.com"));
    }

    #[test]
    fn config_without_network_section_yields_empty() {
        let cfg = write_config("");
        let relays = resolve_relays(None, Some(cfg.path())).unwrap();
        assert!(relays.is_empty());
    }

    #[test]
    fn empty_relays_disable_relay_mode() {
        assert!(matches!(relay_mode(&[]), RelayMode::Disabled));
        let one = vec![RelayUrl::from_str("https://relay.example.com").unwrap()];
        assert!(matches!(relay_mode(&one), RelayMode::Custom(_)));
    }
}
