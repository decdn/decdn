use super::*;
use std::assert_matches;
use std::io::Write;

fn write_config(body: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
    f.write_all(body.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

#[test]
fn client_transport_windows_are_sized_and_ordered() {
    // Pin the configured sizes: the per-stream window matches a default
    // node's `credit_max` ceiling, and the connection window sits above it.
    assert_eq!(CLIENT_STREAM_RECEIVE_WINDOW, 64 * 1024 * 1024);
    assert_eq!(CLIENT_RECEIVE_WINDOW, 128 * 1024 * 1024);
    // The builder accepts these values and yields a config. (The
    // stream-<=-connection invariant is a compile-time assertion above.)
    let _config = client_transport_config();
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
    assert_matches!(relay_mode(&[]), RelayMode::Disabled);
    let one = vec![RelayUrl::from_str("https://relay.example.com").unwrap()];
    assert_matches!(relay_mode(&one), RelayMode::Custom(_));
}
