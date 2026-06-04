//! Shared `Content-Encoding` handling for origin backends.
//!
//! Both [`crate::origin::HttpOrigin`] and [`crate::origin::S3Origin`] serve
//! content-addressed blobs whose BLAKE3 address is computed over the
//! canonical (decompressed) form. When an origin returns a compressed body
//! we must decode it back to canonical bytes *before* the engine hash-
//! verifies, or the verify trips a confusing "hash mismatch". This module
//! centralises the encoding classification, the strict/auto policy, and the
//! gzip/zstd decoder sandwich so the two backends stay byte-for-byte
//! consistent (#312, #804).

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::io::{ReaderStream, StreamReader};

use super::{DecompressMode, OriginByteStream};
use crate::error::{OriginError, SupportedEncoding};

/// Classify a (trimmed) `Content-Encoding` token: identity / supported /
/// unsupported. Returns `None` for the identity case (empty or
/// `identity`), `Some(Ok(_))` for known decoders, and `Some(Err(_))`
/// for unknown encodings. Centralises the case-folding so callers can't
/// disagree on whether `GZIP` is gzip.
///
/// A stacked/multi-value token (e.g. `gzip, gzip` or `gzip, br`) matches
/// none of the single-encoding arms and is deliberately rejected as
/// unsupported: the BLAKE3 address is over the single canonical form, so
/// refusing a doubly-encoded body is the safe choice (no unverified
/// pass-through) rather than attempting to unwrap layers.
fn classify_encoding(trimmed: &str) -> Option<Result<SupportedEncoding, OriginError>> {
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("identity") {
        return None;
    }
    if trimmed.eq_ignore_ascii_case("gzip") || trimmed.eq_ignore_ascii_case("x-gzip") {
        return Some(Ok(SupportedEncoding::Gzip));
    }
    if trimmed.eq_ignore_ascii_case("zstd") {
        return Some(Ok(SupportedEncoding::Zstd));
    }
    Some(Err(OriginError::UnsupportedEncoding {
        encoding: trimmed.into(),
    }))
}

/// Resolve a (trimmed) `Content-Encoding` token against the configured
/// [`DecompressMode`], collapsing the identity / supported / unsupported /
/// strict-rejection cases into a single decision:
///
/// - `Ok(None)` — identity body, stream through unchanged.
/// - `Ok(Some(enc))` — known encoding the caller should decode (only in
///   [`DecompressMode::Auto`]).
/// - `Err(_)` — either an unknown encoding, or a known encoding under
///   [`DecompressMode::Strict`]. The caller surfaces this as a permanent
///   origin error before any body bytes are read.
///
/// Both backends share this so the strict/auto semantics are identical
/// regardless of where the bytes come from.
pub(crate) fn resolve_encoding(
    trimmed: &str,
    mode: DecompressMode,
) -> Result<Option<SupportedEncoding>, OriginError> {
    let supported = match classify_encoding(trimmed) {
        None => return Ok(None),
        Some(Ok(supported)) => supported,
        Some(Err(unsupported)) => return Err(unsupported),
    };
    // Strict mode rejects any non-identity encoding even when we know the
    // decoder. Equivalent to the pre-#312 reject-everything behaviour.
    if matches!(mode, DecompressMode::Strict) {
        return Err(OriginError::UnsupportedEncoding {
            encoding: trimmed.into(),
        });
    }
    Ok(Some(supported))
}

/// Wrap a decoder `io::Error` into an `io::Error` whose source is a
/// typed [`OriginError::DecompressionFailed`]. The engine's
/// `count_and_cap_stream` side-channel preserves the `io::Error`
/// verbatim; `crate::retry::classify_io_error` then downcasts the
/// inner to recover the typed variant and surfaces it as
/// `OriginPullError::Permanent` (`CacheError::origin_error_kind`
/// finds it on the chain walk).
fn typed_decoder_error(encoding: SupportedEncoding, source: std::io::Error) -> std::io::Error {
    let kind = source.kind();
    let typed = OriginError::DecompressionFailed { encoding, source };
    std::io::Error::new(kind, typed)
}

/// Layer the appropriate decoder onto a raw (encoded) byte stream.
///
/// For `encoding == None` the stream is boxed through unchanged. For a
/// known encoding we build a `StreamReader -> bufread decoder ->
/// ReaderStream` sandwich. Decoder errors (truncated body, bad magic,
/// mid-stream checksum mismatch) emerge from `ReaderStream` as
/// `io::Error` and are wrapped with [`typed_decoder_error`] so the typed
/// [`OriginError::DecompressionFailed`] is recoverable downstream.
///
/// The decompressed-side `max_bytes` cap is enforced by the engine's
/// `count_and_cap_stream` (a 1 KB compressed payload that decodes to
/// 100 GB fails fast at the engine seam without pinning that memory), so
/// this helper does not re-implement it.
pub(crate) fn decode_stream<S>(
    raw_stream: S,
    encoding: Option<SupportedEncoding>,
) -> OriginByteStream
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    match encoding {
        None => Box::pin(raw_stream),
        Some(SupportedEncoding::Gzip) => {
            let reader = StreamReader::new(raw_stream);
            let decoder = async_compression::tokio::bufread::GzipDecoder::new(reader);
            Box::pin(
                ReaderStream::new(decoder)
                    .map(|res| res.map_err(|e| typed_decoder_error(SupportedEncoding::Gzip, e))),
            )
        }
        Some(SupportedEncoding::Zstd) => {
            let reader = StreamReader::new(raw_stream);
            let decoder = async_compression::tokio::bufread::ZstdDecoder::new(reader);
            Box::pin(
                ReaderStream::new(decoder)
                    .map(|res| res.map_err(|e| typed_decoder_error(SupportedEncoding::Zstd, e))),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_resolves_known_and_identity_encodings() {
        assert!(matches!(
            resolve_encoding("", DecompressMode::Auto),
            Ok(None)
        ));
        assert!(matches!(
            resolve_encoding("identity", DecompressMode::Auto),
            Ok(None)
        ));
        assert!(matches!(
            resolve_encoding("gzip", DecompressMode::Auto),
            Ok(Some(SupportedEncoding::Gzip))
        ));
        assert!(matches!(
            resolve_encoding("X-GZIP", DecompressMode::Auto),
            Ok(Some(SupportedEncoding::Gzip))
        ));
        assert!(matches!(
            resolve_encoding("ZSTD", DecompressMode::Auto),
            Ok(Some(SupportedEncoding::Zstd))
        ));
    }

    #[test]
    fn unknown_encoding_is_error_in_both_modes() {
        for mode in [DecompressMode::Auto, DecompressMode::Strict] {
            assert!(matches!(
                resolve_encoding("br", mode),
                Err(OriginError::UnsupportedEncoding { .. })
            ));
        }
    }

    #[test]
    fn strict_rejects_known_encodings_but_allows_identity() {
        assert!(matches!(
            resolve_encoding("gzip", DecompressMode::Strict),
            Err(OriginError::UnsupportedEncoding { .. })
        ));
        assert!(matches!(
            resolve_encoding("zstd", DecompressMode::Strict),
            Err(OriginError::UnsupportedEncoding { .. })
        ));
        assert!(matches!(
            resolve_encoding("identity", DecompressMode::Strict),
            Ok(None)
        ));
    }
}
