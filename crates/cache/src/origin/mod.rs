//! Origin backends — the source of truth a node falls back to on a cache miss.
//!
//! [`Origin`] is the seam. Three concrete implementations ship today:
//! [`HttpOrigin`] (generic HTTP/S), [`FilesystemOrigin`] (local disk), and
//! [`S3Origin`] (S3-compatible object stores: AWS S3, R2, B2, `MinIO`).

pub mod fs;
pub mod http;
pub mod s3;

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

pub use fs::FilesystemOrigin;
pub use http::{DEFAULT_USER_AGENT, DecompressMode, HttpOrigin, OriginUrl, parse_origin_url};
pub use s3::{S3Credentials, S3Origin, S3OriginConfig};

use crate::error::OriginPullError;

/// Tag identifying which [`Origin`] backend a [`crate::CacheEngine`] is
/// configured against (#439). Surfaced through
/// [`crate::EvictionPreview::origin_kind`] so admin dry-run callers can
/// estimate origin egress cost — re-fetching from a `Filesystem` origin
/// is a local read; `Http` and `S3` may consume metered bandwidth.
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
    /// Pull-through over HTTP/HTTPS via [`HttpOrigin`].
    Http,
    /// Pull-through from a local filesystem directory via
    /// [`FilesystemOrigin`].
    Filesystem,
    /// Pull-through from an S3-compatible object store via [`S3Origin`].
    S3,
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
        }
    }
}

impl std::fmt::Display for OriginKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of an [`Origin::fetch`] call.
#[derive(Debug)]
pub enum OriginFetch {
    /// The origin returned the bytes for this hash.
    Found(Bytes),
    /// The origin reported the object does not exist (e.g. HTTP 404).
    NotFound,
}

/// An origin backend. Implementors fetch a blob identified by its BLAKE3 hash.
///
/// The origin is **not** responsible for verifying the hash — the cache engine
/// does that after the bytes come back. Returning the wrong bytes is a
/// detectable protocol violation, not a security breach: no data is trusted
/// until it matches the content address.
///
/// ## Error classification
///
/// Adapters return [`OriginPullError::Transient`] for failures that can be
/// retried (HTTP 5xx/408/429, transient I/O), and
/// [`OriginPullError::Permanent`] for failures that won't be cured by
/// retrying (HTTP 4xx other than 404, encoding/cap breaches, permission
/// denied). The cache engine drives the retry loop in
/// [`crate::retry::retry_fetch`] off this distinction. `NotFound` is *not*
/// an error — adapters return [`OriginFetch::NotFound`].
pub trait Origin: std::fmt::Debug + Send + Sync + 'static {
    /// Fetch the blob with the given hash. `max_bytes` is an advisory cap —
    /// implementations should short-circuit if they can cheaply detect the
    /// payload would exceed it (e.g. from a `Content-Length` header), and the
    /// cache engine enforces the cap on the returned bytes regardless.
    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>;

    /// Tag identifying the backend type. Surfaced through
    /// [`crate::EvictionPreview::origin_kind`] so admin dry-run callers
    /// can estimate origin egress cost (#439) — `Filesystem` is a local
    /// read; `Http` may bill metered bandwidth.
    fn kind(&self) -> OriginKind;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
    }

    /// Round-trip via JSON to lock both the serialise *and* deserialise
    /// directions: a regression that only changed one direction (e.g.
    /// renamed a variant but added a `#[serde(alias = ...)]`) would
    /// still drift the operator-visible string.
    #[test]
    fn origin_kind_round_trips_through_json() {
        for variant in [OriginKind::Http, OriginKind::Filesystem, OriginKind::S3] {
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
        for variant in [OriginKind::Http, OriginKind::Filesystem, OriginKind::S3] {
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
