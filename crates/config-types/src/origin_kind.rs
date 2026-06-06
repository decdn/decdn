//! Tag identifying which origin backend a cache engine is configured
//! against (#439).

use serde::{Deserialize, Serialize};

/// Tag identifying which origin backend a cache engine is configured
/// against (#439). Surfaced through the admin eviction-preview so
/// dry-run callers can estimate origin egress cost — re-fetching from a
/// `Filesystem` origin is a local read; `Http` and `S3` may consume
/// metered bandwidth.
///
/// The serde representation uses lowercase tag names (`http`,
/// `filesystem`, `s3`) so admin RPC JSON output is operator-friendly
/// and stable for log scrapers. (`OriginKind` itself is output-only —
/// it is never deserialised from operator TOML; the `[cache.origin]`
/// table uses `OriginConfig`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum OriginKind {
    /// Pull-through over HTTP/HTTPS.
    Http,
    /// Pull-through from a local filesystem directory.
    Filesystem,
    /// Pull-through from an S3-compatible object store.
    S3,
    /// Paid pull-through from another deCDN node over `cdn/client/v1`
    /// (node-to-node cache-miss fill, #831). Unlike the other kinds this
    /// egress is billed in USDC per MB (an upstream provider charges this
    /// node), so an eviction-preview caller should treat a re-fetch from a
    /// `Peer` origin as the most expensive option. This variant is
    /// node-internal — it is never parsed from operator TOML (the
    /// `[cache.origin]` table only accepts `http`/`filesystem`/`s3`); it
    /// only ever appears in admin/eviction output to tag the network origin.
    Peer,
}

impl OriginKind {
    /// Stable lowercase string label suitable for log fields and
    /// metrics. Operators may grep on this — keep the variants stable.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Filesystem => "filesystem",
            Self::S3 => "s3",
            Self::Peer => "peer",
        }
    }
}

impl std::fmt::Display for OriginKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
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
}
