//! Error type for the cache engine.

use std::fmt;

use iroh_blobs::Hash;
use thiserror::Error;

/// Errors surfaced by [`crate::CacheEngine`].
#[derive(Debug, Error)]
pub enum CacheError {
    /// The requested blob was not found locally and the configured origin
    /// returned a not-found response.
    #[error("blob {hash} not found in cache or at origin")]
    NotFound {
        /// The hash that was requested.
        hash: Hash,
    },

    /// The origin returned bytes whose BLAKE3 hash did not match what was
    /// requested. The bytes are rejected and are *not* inserted into the store.
    #[error("origin returned blob {actual} when {expected} was requested")]
    HashMismatch {
        /// The hash the caller asked for.
        expected: Hash,
        /// The hash the origin's bytes actually produced.
        actual: Hash,
    },

    /// The origin returned a blob larger than the configured
    /// `max_blob_size_mb`.
    #[error("blob {hash} exceeds max_blob_size of {limit_bytes} bytes")]
    BlobTooLarge {
        /// The hash that was requested.
        hash: Hash,
        /// The configured ceiling, in bytes.
        limit_bytes: u64,
    },

    /// The cache engine was asked to pull a miss but no origin was configured.
    #[error("cache miss for {hash} but no origin is configured")]
    NoOrigin {
        /// The hash that was requested.
        hash: Hash,
    },

    /// The origin backend failed (network error, non-200 status, etc).
    #[error("origin error fetching {hash}: {source}")]
    OriginError {
        /// The hash that was requested.
        hash: Hash,
        /// Underlying error from the origin backend.
        #[source]
        source: anyhow::Error,
    },

    /// The underlying iroh-blobs store failed.
    #[error("store error: {0}")]
    Store(#[source] anyhow::Error),

    /// The local evicted-hash set is full. Hard cap on the number of
    /// distinct hashes the operator may evict in a single cache lifetime
    /// (issue #279) — protects the in-memory `HashSet` and the on-disk
    /// `evicted.log` from unbounded growth under e.g. an automation
    /// gone-wrong that mass-evicts on every request. Hitting this is
    /// well outside normal usage; the operator should investigate the
    /// caller before raising the cap.
    #[error("evicted-hash set full ({limit} entries); refusing to add more")]
    EvictionLimitExceeded {
        /// The cap that was reached.
        limit: usize,
    },
}

impl CacheError {
    /// Walk the source chain of [`Self::OriginError`] looking for an
    /// [`OriginError`]. Returns `Some` when the failure originated in
    /// the HTTP origin's encoding/decompression layer, even after
    /// [`anyhow::Context::with_context`] wrappers were stacked on top —
    /// `anyhow::Error::chain()` exposes every cause in the chain, and
    /// the typed variant we want lives at the root.
    ///
    /// Tests assert on the typed variant via `matches!`; production
    /// observability code can switch on the variant rather than parsing
    /// `Display` strings. Returns `None` for non-origin failures or
    /// origin failures that didn't come from `HttpOrigin`'s decoder.
    pub fn origin_error_kind(&self) -> Option<&OriginError> {
        let Self::OriginError { source, .. } = self else {
            return None;
        };
        source
            .chain()
            .find_map(|cause| cause.downcast_ref::<OriginError>())
    }
}

/// `Content-Encoding` values [`crate::origin::HttpOrigin`] knows how to
/// decode. Used as the `encoding` field of
/// [`OriginError::DecompressionFailed`] so that variant is structurally
/// reachable only from genuine decoder failures — making
/// `DecompressionFailed { encoding: "lol", … }` unrepresentable by
/// construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportedEncoding {
    /// `Content-Encoding: gzip` (and the legacy `x-gzip` alias).
    Gzip,
    /// `Content-Encoding: zstd`.
    Zstd,
}

impl fmt::Display for SupportedEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gzip => f.write_str("gzip"),
            Self::Zstd => f.write_str("zstd"),
        }
    }
}

/// Errors raised by the [`crate::origin::HttpOrigin`] decompression layer.
///
/// Surfaced to the engine through the `anyhow::Error` source on
/// [`CacheError::OriginError`] (the `Origin` trait returns `anyhow::Result`).
/// Use [`CacheError::origin_error_kind`] to recover the typed variant —
/// the chain walk works through any number of `with_context` wrappers.
#[derive(Debug, Error)]
pub enum OriginError {
    /// The response advertised a `Content-Encoding` value the origin layer
    /// does not understand. We refuse to silently pass the bytes through —
    /// the BLAKE3 verify in the engine would fail, but with a confusing
    /// "hash mismatch" message rather than a precise "your origin is using
    /// an encoding I don't support" message.
    ///
    /// `encoding` is `Box<str>` rather than `String` to keep the variant
    /// 16 bytes on a 64-bit target — error enums get cloned/passed
    /// around the chain a lot, and the underlying buffer is never
    /// resized after construction.
    #[error("unsupported Content-Encoding: {encoding}")]
    UnsupportedEncoding {
        /// The raw header value the origin sent.
        encoding: Box<str>,
    },

    /// The response carried a non-ASCII `Content-Encoding` byte sequence
    /// that we refuse to interpret. RFC 9110 § 5.6.7 restricts coding
    /// names to ASCII tokens; treating an arbitrary byte sequence as
    /// "no encoding" would silently pass through what may be a real
    /// (mis-cased / mis-typed) compression directive.
    #[error("malformed Content-Encoding header (non-ASCII or unprintable)")]
    MalformedEncoding,

    /// Decompression of the response body failed mid-stream. Either the
    /// origin sent a corrupt payload or an encoding mismatch slipped past
    /// the header check (e.g. `Content-Encoding: gzip` but the bytes are
    /// actually zstd). The `encoding` field is typed (closed set) so
    /// nonsense values are unrepresentable — this variant is reachable
    /// only from the gzip and zstd code paths.
    #[error("failed to decompress {encoding} response body: {source}")]
    DecompressionFailed {
        /// The supported encoding whose decoder rejected the body.
        encoding: SupportedEncoding,
        /// Underlying decoder error.
        #[source]
        source: std::io::Error,
    },
}

/// Convenience result alias.
pub type CacheResult<T> = std::result::Result<T, CacheError>;
