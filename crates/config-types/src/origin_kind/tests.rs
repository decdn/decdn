use super::*;

/// `OriginKind`'s JSON serialisation is part of the operator-visible
/// contract — admin RPC responses, log scrapers, and runbook regex
/// patterns all depend on the literal lowercase strings `"http"`,
/// `"filesystem"`, and `"s3"`. A future refactor that swapped
/// `#[serde(rename_all = "lowercase")]` for `snake_case`, dropped the
/// attribute, or accidentally derived a different shape would break
/// every consumer silently. Pin the wire form here so any such
/// regression fails this test.
#[test]
fn origin_kind_serialises_to_stable_lowercase_strings() {
    assert_eq!(
        serde_json::to_string(&OriginKind::Http).expect("serialise"),
        "\"http\""
    );
    assert_eq!(
        serde_json::to_string(&OriginKind::Filesystem).expect("serialise"),
        "\"filesystem\""
    );
    assert_eq!(
        serde_json::to_string(&OriginKind::S3).expect("serialise"),
        "\"s3\""
    );
    assert_eq!(
        serde_json::to_string(&OriginKind::Peer).expect("serialise"),
        "\"peer\""
    );
}

/// Round-trip via JSON to lock both the serialise *and* deserialise
/// directions: a regression that only changed one direction (e.g.
/// renamed a variant but added a `#[serde(alias = ...)]`) would
/// still drift the operator-visible string.
#[test]
fn origin_kind_round_trips_through_json() {
    for variant in [
        OriginKind::Http,
        OriginKind::Filesystem,
        OriginKind::S3,
        OriginKind::Peer,
    ] {
        let s = serde_json::to_string(&variant).expect("serialise");
        let back: OriginKind = serde_json::from_str(&s).expect("deserialise");
        assert_eq!(variant, back, "round-trip for {variant:?}");
    }
}

/// `as_str` and `Display` must agree with the serde tag — operators
/// who grep on `decdn_origin_kind="http"` in tracing log lines see
/// the `Display` form, while admin RPC consumers see the JSON form.
/// The two paths must not drift.
#[test]
fn origin_kind_as_str_and_display_match_serde_tag() {
    for variant in [
        OriginKind::Http,
        OriginKind::Filesystem,
        OriginKind::S3,
        OriginKind::Peer,
    ] {
        let json = serde_json::to_string(&variant).expect("serialise");
        // JSON wraps the tag in quotes; strip them.
        let unquoted = json.trim_matches('"');
        assert_eq!(
            variant.as_str(),
            unquoted,
            "as_str disagrees with serde tag for {variant:?}"
        );
        assert_eq!(
            variant.to_string(),
            unquoted,
            "Display disagrees with serde tag for {variant:?}"
        );
    }
}
