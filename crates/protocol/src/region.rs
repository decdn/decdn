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

#[cfg(test)]
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
}
