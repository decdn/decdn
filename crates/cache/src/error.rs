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

/// Convenience result alias.
pub type CacheResult<T> = std::result::Result<T, CacheError>;
