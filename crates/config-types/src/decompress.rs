//! `Content-Encoding` handling mode for the HTTP origin.

use serde::{Deserialize, Serialize};

/// How the HTTP origin handles `Content-Encoding` on the response.
///
/// `bool` was the original config knob, but the two states have richer
/// semantics than "on / off" — `Strict` is not "no decompression", it
/// is "I will refuse anything other than identity". Using a typed enum
/// keeps that distinction visible at every call site (config, runtime,
/// fetch path) and on operator-facing config files.
///
/// The TOML representation uses lowercase tag names: `decompress = "auto"`
/// or `decompress = "strict"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecompressMode {
    /// Decode `gzip` / `zstd` bodies transparently; identity passes
    /// through; unknown encodings raise `OriginError::UnsupportedEncoding`.
    /// This is the default — most object stores serve compressed bodies
    /// and the BLAKE3 verify in the cache engine runs over the canonical
    /// (decompressed) form, so pass-through would always fail
    /// verification.
    #[default]
    Auto,
    /// Reject any non-identity `Content-Encoding`. The origin must serve
    /// canonical bytes; gzip / zstd / unknown encodings all return
    /// `OriginError::UnsupportedEncoding` before any decode runs.
    /// Useful only for origins that pre-canonicalise (e.g. an internal
    /// pre-warmed mirror).
    Strict,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests {
    use super::*;

    /// The TOML/JSON tag form is operator-visible config surface — pin
    /// the lowercase `rename_all` contract so a future refactor that
    /// drops or changes the attribute fails here rather than silently
    /// breaking every operator's `decompress = "auto"`.
    #[test]
    fn serialises_to_lowercase_tags() {
        assert_eq!(
            serde_json::to_string(&DecompressMode::Auto).expect("serialise"),
            "\"auto\""
        );
        assert_eq!(
            serde_json::to_string(&DecompressMode::Strict).expect("serialise"),
            "\"strict\""
        );
        let back: DecompressMode = serde_json::from_str("\"strict\"").expect("deserialise");
        assert_eq!(back, DecompressMode::Strict);
        assert_eq!(DecompressMode::default(), DecompressMode::Auto);
    }
}
