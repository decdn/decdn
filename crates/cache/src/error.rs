//! Error type for the cache engine.

use std::fmt;

use iroh_blobs::Hash;
use thiserror::Error;

/// Errors surfaced by [`crate::CacheEngine`].
///
/// A variant that wraps a cause names only itself in `Display` and returns the
/// cause from [`std::error::Error::source`]. A `.chain()` walk therefore reaches
/// every cause under a `CacheError`, such as a local disk fault that
/// `decdn_client::classify` looks for, and a chain formatter (`{:#}` or `{:?}` on an
/// `anyhow::Error`) names each cause once. Log a bare `CacheError` with
/// [`CacheError::display_chain`], not with plain `Display`.
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
    /// A whole-stream failure: the assembled bytes hashed to `actual`, not the
    /// requested `expected`. A mid-stream bao chunk-group verify failure surfaces
    /// as [`Self::VerifyFailed`] instead, so `actual` here is always a real hash.
    #[error("origin returned blob {actual} when {expected} was requested")]
    HashMismatch {
        /// The hash the caller asked for.
        expected: Hash,
        /// The hash the origin's bytes actually produced.
        actual: Hash,
    },

    /// A teed bao verified-stream (#856, ADR 038) failed to decode against the
    /// content root: an interior chunk group did not verify — a corrupt or lying
    /// upstream. Distinct from [`Self::HashMismatch`], which is a whole-stream
    /// hash mismatch; here the failure is at an interior group, so there is no
    /// meaningful whole-blob "actual" hash to report (#915). The bytes are
    /// rejected and are *not* inserted into the store.
    #[error("origin bytes failed bao verification against {expected}")]
    VerifyFailed {
        /// The hash the caller asked for (the content root the stream failed
        /// to verify against).
        expected: Hash,
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
    #[error("origin error fetching {hash}")]
    OriginError {
        /// The hash that was requested.
        hash: Hash,
        /// Underlying error from the origin backend.
        #[source]
        source: anyhow::Error,
    },

    /// This node's own iroh-blobs store failed, or a store operation asked for
    /// bytes the store does not hold. Always a local fault: the bytes a sender
    /// feeds into an admit failing to arrive is [`Self::Feed`], and bytes that
    /// fail to verify are [`Self::VerifyFailed`].
    #[error("store error")]
    Store(#[source] anyhow::Error),

    /// The bao stream fed into an admit ended early or failed to read, or its
    /// claimed size cannot belong to the hash. A fault of the sender's delivery,
    /// not of this node's store.
    #[error("admit feed failed")]
    Feed(#[source] anyhow::Error),

    /// A code bug in the cache: a broken internal invariant or a caught panic.
    /// Never an origin fault and never a store fault, so an operator reads it
    /// as a bug to report, not as a backend to check.
    #[error("internal fault")]
    Internal(#[source] anyhow::Error),

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
    /// Render this error and every cause under it on one line
    /// (`store error: context: root cause`). Use it wherever a bare
    /// `CacheError` reaches a log line or an operator-facing message.
    pub fn display_chain(&self) -> ErrorChain<'_> {
        ErrorChain::new(self)
    }
}

/// Renders any error and each [`std::error::Error::source`] under it on one
/// line, joined by `": "`. Built by [`CacheError::display_chain`], or by
/// [`ErrorChain::new`] for any other error.
///
/// Each cause appears once only when every link leaves its `source()` out of
/// its own `Display`, as [`CacheError`] does. A link that repeats its source in
/// `Display` repeats that text here.
#[derive(Debug, Clone, Copy)]
pub struct ErrorChain<'a>(&'a (dyn std::error::Error + 'static));

impl<'a> ErrorChain<'a> {
    /// Wrap `err` for one-line chain rendering.
    pub const fn new(err: &'a (dyn std::error::Error + 'static)) -> Self {
        Self(err)
    }
}

impl fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut cause = self.0.source();
        while let Some(err) = cause {
            write!(f, ": {err}")?;
            cause = err.source();
        }
        Ok(())
    }
}

/// Convenience result alias.
pub type CacheResult<T> = std::result::Result<T, CacheError>;

/// A failed origin read, classified by whether a retry is appropriate. The
/// cache engine's retry loop drives off this distinction — `Transient`
/// failures are retried up to [`crate::RetryPolicy::max_retries`], `Permanent`
/// failures surface to the caller immediately.
///
/// Both variants carry an [`anyhow::Error`] so adapters keep their
/// `with_context` chains intact. The retry loop, the circuit breaker and the
/// presence probe read the classification. A caller that surfaces the failure
/// drops it with [`Self::into_inner`] and returns the cause as
/// [`CacheError::OriginError`].
#[derive(Debug, Error)]
pub enum OriginPullError {
    /// A retry-eligible failure: HTTP 5xx/408/429, connect/headers/chunk
    /// timeouts, transport-level connection resets, or transient I/O on the
    /// filesystem origin (`Interrupted`, `TimedOut`, `ResourceBusy`,
    /// `WouldBlock`, `StaleNetworkFileHandle`).
    #[error(transparent)]
    Transient(anyhow::Error),

    /// A failure that won't be cured by retrying: HTTP 4xx (other than 404,
    /// which each read answers as a missing object), an object over its cap
    /// or shorter than the requested span, or a symlink escape, a dangling
    /// symlink or a permission denied on the filesystem origin.
    #[error(transparent)]
    Permanent(anyhow::Error),
}

impl OriginPullError {
    /// True when the variant is `Transient` — the retry loop tests this in
    /// the hot path; keep it `const` so it inlines cleanly.
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    /// Unwrap the underlying `anyhow::Error`, discarding the
    /// transient/permanent classification. Used by the cache engine when
    /// collapsing into [`CacheError::OriginError`] after retries are
    /// exhausted (or on the first permanent failure).
    pub fn into_inner(self) -> anyhow::Error {
        match self {
            Self::Transient(e) | Self::Permanent(e) => e,
        }
    }

    /// Apply `f` to the inner `anyhow::Error` while preserving the
    /// transient/permanent classification. Lets adapters stack
    /// `with_context` (or any other error transform) onto a typed error
    /// without flipping it to the wrong variant.
    pub(crate) fn map_inner<F>(self, f: F) -> Self
    where
        F: FnOnce(anyhow::Error) -> anyhow::Error,
    {
        match self {
            Self::Transient(e) => Self::Transient(f(e)),
            Self::Permanent(e) => Self::Permanent(f(e)),
        }
    }
}

#[cfg(test)]
mod tests;
