//! Error type for the cache engine.

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
}

/// Errors raised by the [`crate::origin::HttpOrigin`] decompression layer.
///
/// Surfaced to the engine through the `anyhow::Error` source on
/// [`CacheError::OriginError`] (the `Origin` trait returns `anyhow::Result`).
/// Kept as a typed enum so origin-side decoders can pattern-match in tests
/// and callers can downcast for diagnostics.
#[derive(Debug, Error)]
pub enum OriginError {
    /// The response advertised a `Content-Encoding` value the origin layer
    /// does not understand. We refuse to silently pass the bytes through —
    /// the BLAKE3 verify in the engine would fail, but with a confusing
    /// "hash mismatch" message rather than a precise "your origin is using
    /// an encoding I don't support" message.
    #[error("unsupported Content-Encoding: {encoding}")]
    UnsupportedEncoding {
        /// The raw header value the origin sent.
        encoding: String,
    },

    /// Decompression of the response body failed mid-stream. Either the
    /// origin sent a corrupt payload or an encoding mismatch slipped past
    /// the header check (e.g. `Content-Encoding: gzip` but the bytes are
    /// actually zstd).
    #[error("failed to decompress {encoding} response body: {source}")]
    DecompressionFailed {
        /// The encoding the origin advertised.
        encoding: String,
        /// Underlying decoder error.
        #[source]
        source: std::io::Error,
    },
}

/// Convenience result alias.
pub type CacheResult<T> = std::result::Result<T, CacheError>;
