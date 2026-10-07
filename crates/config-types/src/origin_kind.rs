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
mod tests;
