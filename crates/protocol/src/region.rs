//! ISO 3166-1 alpha-2 region-code validation.
//!
//! `identity.region` (ADR 030) ends up in log fields and metric labels, so
//! this allowlist is what stops arbitrary bytes (control chars, non-ASCII,
//! oversized strings) from corrupting them. The config resolver
//! (`decdn_common::config::normalize_region`) calls [`is_valid_region`] so
//! every accepted region code is one of this set.
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
/// re-derive that: `decdn-client`'s candidate ranking was trimming and
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
    /// resolution — say so at their own layer.
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
mod tests;
