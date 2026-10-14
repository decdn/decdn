use super::*;

const HEX: &str = "ce88d839ce93b3d62c500567602ea372a9b6b043866bdf327e508f7226efe06e";

fn hash() -> [u8; 32] {
    *blake3::Hash::from_hex(HEX).unwrap().as_bytes()
}

fn parse(s: &str) -> Result<DecdnUri, UriError> {
    s.parse()
}

#[test]
fn parses_every_target_kind() {
    assert_eq!(
        parse(&format!("decdn:{HEX}")).unwrap(),
        DecdnUri::new(hash(), UriTarget::Blob, None)
    );
    assert_eq!(
        parse(&format!("decdn:{HEX}/")).unwrap(),
        DecdnUri::new(hash(), UriTarget::Bundle, None)
    );
    assert_eq!(
        parse(&format!("decdn:{HEX}/a/b.txt")).unwrap(),
        DecdnUri::new(hash(), UriTarget::BundleEntry("a/b.txt".into()), None)
    );
}

#[test]
fn parses_the_namespace_hint_on_every_target_kind() {
    for (suffix, target) in [
        ("", UriTarget::Blob),
        ("/", UriTarget::Bundle),
        ("/a/b.txt", UriTarget::BundleEntry("a/b.txt".into())),
    ] {
        let uri = parse(&format!("decdn:{HEX}{suffix}?ns=42")).unwrap();
        assert_eq!(uri, DecdnUri::new(hash(), target, Some(42)));
    }
}

#[test]
fn canonical_form_round_trips() {
    for s in [
        format!("decdn:{HEX}"),
        format!("decdn:{HEX}/"),
        format!("decdn:{HEX}/a/b.txt"),
        format!("decdn:{HEX}/a/b.txt?ns=42"),
        format!("decdn:{HEX}/?ns=1"),
        format!("decdn:{HEX}/my%20model/weights%5B0%5D.bin"),
        format!("decdn:{HEX}/caf%C3%A9.txt"),
    ] {
        let uri = parse(&s).unwrap();
        assert_eq!(uri.to_string(), s);
        assert_eq!(parse(&uri.to_string()).unwrap(), uri);
    }
}

#[test]
fn uppercase_hex_parses_and_prints_lowercase() {
    let upper = format!("decdn:{}", HEX.to_ascii_uppercase());
    let uri = parse(&upper).unwrap();
    assert_eq!(uri.hash, hash());
    assert_eq!(uri.to_string(), format!("decdn:{HEX}"));
}

#[test]
fn percent_encoded_segments_decode_to_the_literal_manifest_path() {
    let uri = parse(&format!("decdn:{HEX}/dir%20one/%5Bx%5D*%3F%7B%7D.bin")).unwrap();
    assert_eq!(
        uri.target,
        UriTarget::BundleEntry("dir one/[x]*?{}.bin".into())
    );
}

#[test]
fn display_encodes_what_is_not_a_pchar() {
    let uri = DecdnUri::new(
        hash(),
        UriTarget::BundleEntry("dir one/[x]?#%.bin".into()),
        None,
    );
    assert_eq!(
        uri.to_string(),
        format!("decdn:{HEX}/dir%20one/%5Bx%5D%3F%23%25.bin")
    );
}

#[test]
fn rejects_a_missing_scheme_or_an_authority() {
    assert_eq!(parse(HEX), Err(UriError::MissingScheme));
    assert_eq!(parse(&format!("decdn://{HEX}")), Err(UriError::Authority));
}

#[test]
fn rejects_prefixed_short_long_or_non_hex_hashes() {
    for bad in [
        format!("decdn:b3:{HEX}"),
        format!("decdn:0x{HEX}"),
        format!("decdn:{}", &HEX[..63]),
        format!("decdn:{HEX}0"),
        format!("decdn:{}g", &HEX[..63]),
    ] {
        assert!(matches!(parse(&bad), Err(UriError::Hash(_))), "{bad}");
    }
}

#[test]
fn rejects_bad_namespaces() {
    for bad in [
        "ns=0",
        "ns=",
        "ns=-1",
        "ns=+1",
        "ns=1.5",
        "ns=x",
        "ns=18446744073709551616",
    ] {
        let s = format!("decdn:{HEX}?{bad}");
        assert!(matches!(parse(&s), Err(UriError::Namespace(_))), "{s}");
    }
}

#[test]
fn rejects_unknown_duplicate_or_empty_query() {
    for bad in ["ns=1&x=2", "x=1", "ns=1&ns=2", "", "ns"] {
        let s = format!("decdn:{HEX}?{bad}");
        assert!(matches!(parse(&s), Err(UriError::Query(_))), "{s}");
    }
}

#[test]
fn rejects_unsafe_or_malformed_entry_paths() {
    for bad in [
        "/../x", "/a/../b", "/./a", "//a", "/a//b", "/a/", "/a%2Fb", "/a%00b", "/a b", "/%zz",
        "/%4", "/%FF", "/a#b",
    ] {
        let s = format!("decdn:{HEX}{bad}");
        assert!(matches!(parse(&s), Err(UriError::Path(_))), "{s}");
    }
}
