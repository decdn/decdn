//! ISO 3166-1 alpha-2 region-code validation.
//!
//! `NodeAnnounce.region` is concatenated into the gossip topic string
//! (`cdn/region/{region}/v1`), so this allowlist is what stops arbitrary
//! bytes (slashes, control chars, non-ASCII) from corrupting topic names,
//! log fields, and metric labels. Both the receive-side validator
//! (`decdn_gossip::validation`) and the publish-side config resolver
//! (`decdn_common::config::normalize_region`) call [`is_valid_region`]
//! so the wire and config layers agree on the acceptable set.
//!
//! Accepted set:
//! - All currently-assigned ISO 3166-1 alpha-2 codes, plus the
//!   exceptionally-reserved `XK` (Kosovo) which is widely used by EU/IMF.
//! - The user-assigned reserved ranges (`AA`, `QM`–`QZ`, `XA`–`XZ`, `ZZ`)
//!   so operators can pick a private code for air-gapped or testnet
//!   deployments without coordinating with ISO.
//!
//! Case-sensitive — callers must uppercase first. The 2-char length is
//! implicit in the match patterns; any other length is rejected.
//!
//! [`Region`] is the parsed form: trim + uppercase + this allowlist, done once
//! at the boundary so consumers compare values rather than re-normalizing at
//! every call site (#1348). Use it wherever a validated code is carried around;
//! [`is_valid_region`] remains for callers that only need the predicate over a
//! string they already own.

/// True iff `code` is a region the network accepts. See module docs for the
/// accepted set.
pub fn is_valid_region(code: &str) -> bool {
    matches!(
        code,
        // Assigned ISO 3166-1 alpha-2 (A–B)
        "AD" | "AE" | "AF" | "AG" | "AI" | "AL" | "AM" | "AO" | "AQ" | "AR"
        | "AS" | "AT" | "AU" | "AW" | "AX" | "AZ"
        | "BA" | "BB" | "BD" | "BE" | "BF" | "BG" | "BH" | "BI" | "BJ" | "BL"
        | "BM" | "BN" | "BO" | "BQ" | "BR" | "BS" | "BT" | "BV" | "BW" | "BY" | "BZ"
        // C–F
        | "CA" | "CC" | "CD" | "CF" | "CG" | "CH" | "CI" | "CK" | "CL" | "CM"
        | "CN" | "CO" | "CR" | "CU" | "CV" | "CW" | "CX" | "CY" | "CZ"
        | "DE" | "DJ" | "DK" | "DM" | "DO" | "DZ"
        | "EC" | "EE" | "EG" | "EH" | "ER" | "ES" | "ET"
        | "FI" | "FJ" | "FK" | "FM" | "FO" | "FR"
        // G–J
        | "GA" | "GB" | "GD" | "GE" | "GF" | "GG" | "GH" | "GI" | "GL" | "GM"
        | "GN" | "GP" | "GQ" | "GR" | "GS" | "GT" | "GU" | "GW" | "GY"
        | "HK" | "HM" | "HN" | "HR" | "HT" | "HU"
        | "ID" | "IE" | "IL" | "IM" | "IN" | "IO" | "IQ" | "IR" | "IS" | "IT"
        | "JE" | "JM" | "JO" | "JP"
        // K–N
        | "KE" | "KG" | "KH" | "KI" | "KM" | "KN" | "KP" | "KR" | "KW" | "KY" | "KZ"
        | "LA" | "LB" | "LC" | "LI" | "LK" | "LR" | "LS" | "LT" | "LU" | "LV" | "LY"
        | "MA" | "MC" | "MD" | "ME" | "MF" | "MG" | "MH" | "MK" | "ML" | "MM"
        | "MN" | "MO" | "MP" | "MQ" | "MR" | "MS" | "MT" | "MU" | "MV" | "MW"
        | "MX" | "MY" | "MZ"
        | "NA" | "NC" | "NE" | "NF" | "NG" | "NI" | "NL" | "NO" | "NP" | "NR"
        | "NU" | "NZ"
        // O–S
        | "OM"
        | "PA" | "PE" | "PF" | "PG" | "PH" | "PK" | "PL" | "PM" | "PN" | "PR"
        | "PS" | "PT" | "PW" | "PY"
        | "QA"
        | "RE" | "RO" | "RS" | "RU" | "RW"
        | "SA" | "SB" | "SC" | "SD" | "SE" | "SG" | "SH" | "SI" | "SJ" | "SK"
        | "SL" | "SM" | "SN" | "SO" | "SR" | "SS" | "ST" | "SV" | "SX" | "SY" | "SZ"
        // T–Z
        | "TC" | "TD" | "TF" | "TG" | "TH" | "TJ" | "TK" | "TL" | "TM" | "TN"
        | "TO" | "TR" | "TT" | "TV" | "TW" | "TZ"
        | "UA" | "UG" | "UM" | "US" | "UY" | "UZ"
        | "VA" | "VC" | "VE" | "VG" | "VI" | "VN" | "VU"
        | "WF" | "WS"
        | "YE" | "YT"
        | "ZA" | "ZM" | "ZW"
        // Exceptionally reserved (widely used by EU/IMF/UNMIK).
        | "XK"
        // User-assigned reserved ranges (ISO 3166-1 §8.1.3). Operators can
        // pick any of these for private deployments without coordinating
        // with ISO.
        | "AA"
        | "QM" | "QN" | "QO" | "QP" | "QQ" | "QR" | "QS" | "QT"
        | "QU" | "QV" | "QW" | "QX" | "QY" | "QZ"
        | "XA" | "XB" | "XC" | "XD" | "XE" | "XF" | "XG" | "XH"
        | "XI" | "XJ" | "XL" | "XM" | "XN" | "XO" | "XP" | "XQ"
        | "XR" | "XS" | "XT" | "XU" | "XV" | "XW" | "XX" | "XY" | "XZ"
        | "ZZ"
    )
}

/// A region code that has already passed [`is_valid_region`], stored in the
/// normalized (uppercase, untrimmed-of-nothing) form.
///
/// The point of the type is that the normalization is done ONCE, at the parse
/// boundary, instead of at every comparison site. A raw `String` carrying "an
/// ISO 3166-1 alpha-2 code" by doc comment alone leaves each consumer to
/// re-derive that: `client-pull`'s candidate ranking was trimming and
/// case-folding on every comparison because one side came normalized from
/// config and the other raw from the chain, and structural `Eq` on the
/// containing type was wrong as a result — `" us "` and `"US"` compared
/// unequal (#1348).
///
/// Two bytes, `Copy`, and bounded by construction — which also caps what an
/// operator-submitted `regionHint` can cost on a deserialize path, where the
/// on-chain source permits any string up to 16 bytes.
///
/// Serializes as the plain 2-character string, so persisted forms stay
/// human-readable and a `Region` field is wire-compatible with the `String` it
/// replaces in the serialize direction.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Region([u8; 2]);

/// Hand-written so the code renders as text, not as its bytes. A derived
/// `Debug` over the `[u8; 2]` payload prints `Region([85, 83])`, and this type
/// IS formatted with `{:?}` on an operator-facing path — `decdn fetch`'s
/// "discovered node … (region {:?})" line — as well as into `tracing` fields
/// and `anyhow` context. The derive turned that line's output from `"US"` into
/// `Some(Region([85, 83]))`.
impl core::fmt::Debug for Region {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Region").field(&self.as_str()).finish()
    }
}

impl Region {
    /// Parse `raw` into a `Region`, or `None` if it is not an accepted code.
    ///
    /// Surrounding whitespace is trimmed and the code is uppercased before the
    /// allowlist check, because both sources this reads from are
    /// operator-submitted: a config file and an on-chain `regionHint` the
    /// contract does not constrain beyond a length cap.
    ///
    /// `None` rather than an error type: for the advisory uses (locality-aware
    /// node ranking) an unrecognized code means "no locality information", not
    /// "reject this node". Callers that need it to be fatal — config
    /// resolution, gossip envelope validation — say so at their own layer.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let upper = raw.trim().to_ascii_uppercase();
        if !is_valid_region(&upper) {
            return None;
        }
        // `is_valid_region` matches only 2-byte ASCII patterns, so the array
        // conversion cannot fail; `ok()?` keeps that fact local instead of
        // asserting it (the workspace denies `expect`/`panic`).
        let bytes: [u8; 2] = upper.as_bytes().try_into().ok()?;
        Some(Self(bytes))
    }

    /// The normalized code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Every byte came from `to_ascii_uppercase` over an allowlisted ASCII
        // code, so this is always valid UTF-8.
        core::str::from_utf8(&self.0).unwrap_or("")
    }
}

impl AsRef<str> for Region {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl core::fmt::Display for Region {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Rejects with the same allowlist [`Region::parse`] applies. The error carries
/// no input — a region code reaches log fields and metric labels, and this type
/// exists partly to keep unvalidated bytes out of them. The `Deserialize` impl
/// below upholds the same property, so it holds for every fallible construction
/// path, not just this one.
impl core::str::FromStr for Region {
    type Err = InvalidRegion;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s).ok_or(InvalidRegion)
    }
}

/// [`Region`]'s [`FromStr`](core::str::FromStr) was given something outside
/// the accepted set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRegion;

impl core::fmt::Display for InvalidRegion {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(
            "not an accepted ISO 3166-1 alpha-2 region code \
             (assigned, or user-reserved AA/QM-QZ/XA-XZ/ZZ)",
        )
    }
}

impl core::error::Error for InvalidRegion {}

impl serde::Serialize for Region {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Deserializing re-runs the allowlist, so the invariant holds for values that
/// arrive from a file or the wire rather than through [`Region::parse`] — a
/// derived impl over the byte array would let any two bytes in.
///
/// Deserializes an owned `String` rather than a borrowed `&str` so this works
/// against non-borrowing formats too (a `serde_json` reader, postcard over a
/// streamed buffer); the allocation is bounded by the caller's own input and
/// dropped immediately.
impl<'de> serde::Deserialize<'de> for Region {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        // `custom(InvalidRegion)`, NOT `invalid_value(Unexpected::Str(&raw))`:
        // the rejected input must not travel in the error, for the same reason
        // `FromStr` refuses to echo it. This path is the one that matters most —
        // it reads the peer cache, whose contents originate from an on-chain
        // `regionHint` any operator can set to arbitrary bytes, and the decode
        // error surfaces to the user through `CacheRead::Unusable`.
        Self::parse(&raw).ok_or_else(|| serde::de::Error::custom(InvalidRegion))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
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
    /// It used to use `Unexpected::Str(&raw)`, which echoed.
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
    /// readable and stay compatible with the `String` this replaced.
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
}
