use super::*;

#[test]
fn accepts_assigned() {
    for code in ["US", "DE", "JP", "GB", "TW", "XK", "OM", "QA"] {
        assert!(is_valid_region(code), "{code} should be accepted");
    }
}

#[test]
fn accepts_user_reserved() {
    // Spot-check each reserved range so a future edit dropping a
    // boundary code fails here, not silently in operator deployments.
    for code in ["AA", "QM", "QN", "QZ", "XA", "XK", "XL", "XX", "XZ", "ZZ"] {
        assert!(is_valid_region(code), "{code} should be accepted");
    }
}

#[test]
fn rejects_unassigned_uppercase() {
    // All 2-letter ASCII uppercase that are neither assigned nor in a
    // reserved range. Old impl accepted these because they passed the
    // bare "2 ASCII uppercase" check.
    for code in ["OO", "JJ", "BX", "WW", "OP", "OZ"] {
        assert!(!is_valid_region(code), "{code} should be rejected");
    }
}

#[test]
fn rejects_malformed() {
    // Length / case / charset failures. Adversarial inputs (slashes,
    // null bytes, control chars, non-ASCII) are explicit because they
    // are the security-bug class the receive-side validator is meant
    // to stop from reaching the topic-name builder.
    for code in [
        "", "U", "USA", "us", "Us", "uS", "U S", "U/", "/U", "U\0", "U\n", "\0\0", "Ü1", "1U",
        "12", "U-",
    ] {
        assert!(!is_valid_region(code), "{code:?} should be rejected");
    }
}

// ---- `Region` (#1348) ----

/// The whole point of the type: normalization happens once, so two
/// spellings of the same region are the SAME value. Before this, the
/// containing type's derived `Eq` compared `" us "` and `"US"` unequal.
#[test]
fn parse_normalizes_case_and_whitespace_into_one_value() {
    let canonical = Region::parse("US").expect("US is assigned");
    for spelling in ["us", " US ", "\tuS\n", "Us"] {
        assert_eq!(
            Region::parse(spelling),
            Some(canonical),
            "{spelling:?} must normalize to the same value as \"US\""
        );
    }
    assert_eq!(canonical.as_str(), "US");
    assert_eq!(canonical.to_string(), "US");
}

/// `Debug` renders the code, not the byte payload. This is not cosmetic:
/// `decdn fetch` prints the discovered node's region with `{:?}`, so a
/// derived `Debug` shows the operator `Some(Region([85, 83]))`.
#[test]
fn debug_renders_the_code_not_the_bytes() {
    let region = Region::parse("US").expect("US is assigned");
    assert_eq!(format!("{region:?}"), r#"Region("US")"#);
    assert_eq!(format!("{:?}", Some(region)), r#"Some(Region("US"))"#);
}

/// Proves the two "unreachable" fallbacks — `parse`'s `ok()?` on the array
/// conversion and `as_str`'s `unwrap_or("")` — are actually unreachable,
/// rather than arguing it in a comment. Every code the allowlist accepts
/// round-trips to itself, so neither branch can fire.
///
/// Doubles as a tripwire on the allowlist: a bad edit that collapsed the
/// `matches!` arms would fail the count assertion rather than silently
/// shrinking the set of regions the network accepts.
#[test]
fn every_accepted_code_round_trips_and_is_never_empty() {
    let mut accepted = 0usize;
    for a in b'A'..=b'Z' {
        for b in b'A'..=b'Z' {
            let code = String::from_utf8(vec![a, b]).expect("ASCII by construction");
            if !is_valid_region(&code) {
                continue;
            }
            accepted += 1;
            let region = Region::parse(&code).expect("the predicate accepted it");
            assert_eq!(
                region.as_str(),
                code,
                "as_str must be total — never the empty fallback"
            );
        }
    }
    assert!(
        accepted > 240,
        "the allowlist collapsed to {accepted} codes; ISO 3166-1 assigns ~250"
    );
}

/// Parsing is exactly as permissive as the predicate — no more (a code the
/// network rejects must not become a `Region`) and no less (the
/// user-reserved ranges are what private deployments run on).
#[test]
fn parse_agrees_with_is_valid_region() {
    for code in ["US", "DE", "XK", "AA", "QZ", "ZZ"] {
        assert!(Region::parse(code).is_some(), "{code} should parse");
    }
    for code in ["OO", "JJ", "", "U", "USA", "U/", "U\0", "Ü1", "12"] {
        assert!(Region::parse(code).is_none(), "{code:?} should not parse");
    }
}

/// NO fallible construction path may echo the rejected input back: a region
/// code lands in log fields and metric labels, and keeping unvalidated bytes
/// out of those is half of why the allowlist exists.
///
/// `Deserialize` is asserted alongside `FromStr` because it is the path that
/// actually reads untrusted data — the peer cache, whose region strings come
/// from an on-chain `regionHint` any operator can set to arbitrary bytes,
/// and whose decode error reaches the user through `CacheRead::Unusable`.
/// The deserializer must not surface the raw input (e.g. via
/// `Unexpected::Str(&raw)`), which would echo it.
#[test]
fn construction_errors_do_not_echo_the_input() {
    let from_str = "U\n/evil".parse::<Region>().expect_err("must reject");
    let msg = from_str.to_string();
    assert!(
        !msg.contains("evil"),
        "input leaked into the message: {msg}"
    );
    assert!(msg.contains("ISO 3166-1"), "{msg}");

    let de = serde_json::from_str::<Region>("\"U\\n/evil\"").expect_err("must reject");
    let msg = de.to_string();
    assert!(
        !msg.contains("evil"),
        "input leaked through the deserializer: {msg}"
    );
    assert!(msg.contains("ISO 3166-1"), "{msg}");
}

/// Round-trips as the plain 2-character string, so persisted forms stay
/// readable.
#[test]
fn serializes_as_a_plain_string() {
    let region = Region::parse("DE").expect("DE is assigned");
    let json = serde_json::to_string(&region).expect("serialize");
    assert_eq!(json, "\"DE\"");
    assert_eq!(
        serde_json::from_str::<Region>(&json).expect("round-trip"),
        region
    );
}

/// The allowlist runs on the way IN as well. A derived impl over the byte
/// array would let a peer cache (or any other persisted form) reintroduce
/// an arbitrary two bytes past the parse boundary.
#[test]
fn deserialize_rejects_an_invalid_code() {
    assert!(serde_json::from_str::<Region>("\"OO\"").is_err());
    assert!(serde_json::from_str::<Region>("\"USA\"").is_err());
    // Normalization applies here too, so a lowercased persisted value is
    // read back as the canonical one rather than rejected.
    assert_eq!(
        serde_json::from_str::<Region>("\"de\"").expect("normalizes on the way in"),
        Region::parse("DE").expect("DE is assigned")
    );
}
