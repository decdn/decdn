use super::*;

/// `[client] region_allowlist = [...]` round-trips through TOML into
/// `Some`, carrying the raw (unvalidated) strings — parsing into
/// [`decdn_protocol::Region`] happens at the client's discovery boundary,
/// not here.
#[test]
fn client_region_allowlist_parses_when_present() {
    let file: FileConfig = toml::from_str(
        r#"
            [client]
            region_allowlist = ["US", "DE"]
            "#,
    )
    .expect("valid [client] table parses");
    assert_eq!(
        file.client
            .expect("[client] present")
            .region_allowlist
            .expect("region_allowlist present"),
        vec!["US".to_string(), "DE".to_string()]
    );
}

/// An absent `[client]` table resolves to `None` — no filter, not an
/// empty one — so a config file that omits `[client]` keeps behaving
/// exactly as one that never had a region filter.
#[test]
fn client_section_absent_by_default() {
    let file: FileConfig = toml::from_str("").expect("empty config parses");
    assert!(file.client.is_none());
}
