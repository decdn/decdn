//! [`SecretString`] — a `String` newtype with redacted [`Debug`].
//!
//! Used by [`super::types::S3Credentials::Static`] to hold AWS access
//! keys, secret keys, and session tokens. The wrapper exists so that an
//! incidental `tracing::debug!(?cfg)`, panic backtrace formatter, or
//! stray `dbg!()` cannot leak the credential into logs. The codebase
//! already has redaction discipline for HTTP-origin URL credentials
//! (see `decdn_config_types::redact_for_log`) — this is the same pattern for
//! TOML-borne secrets.
//!
//! - [`std::fmt::Debug`] always prints `"***"` regardless of the
//!   wrapped value.
//! - `std::fmt::Display` is **not** implemented — use
//!   [`SecretString::expose`] when you need the raw value, so every
//!   call site is greppable.
//! - [`serde::Deserialize`] is implemented (so TOML still works).
//! - [`serde::Serialize`] is implemented but emits a redacted,
//!   hashed form (`"hash:{digest}"`) — never the cleartext secret.
//!   The config section structs derive `Serialize` (for symmetry with
//!   `Deserialize`), so `SecretString` must implement it for them to
//!   compile; emitting a hash rather than the raw string is
//!   defense-in-depth, the `Serialize` analogue of the `Debug`
//!   redaction above — any incidental serialization of a config
//!   section (diagnostics, tests, future call sites) cannot leak the
//!   credential. The hash uses
//!   [`std::collections::hash_map::DefaultHasher`], which `std`
//!   documents as producing the same output for all `DefaultHasher`
//!   instances in a given build of the standard library — i.e.
//!   deterministic across processes built with the same toolchain, so
//!   distinct secrets almost always serialize to distinct forms. It is
//!   non-cryptographic (single-pair collision probability ≈ 2⁻⁶⁴ for a
//!   64-bit hash); cryptographic strength is unnecessary because the
//!   output only guards against accidental cleartext exposure, never
//!   authenticates anything.

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize, Serializer};

/// String newtype that redacts itself in [`Debug`] output. See module
/// docs for the threat model.
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    /// Construct a [`SecretString`] from any [`String`]-shaped value.
    /// Most call sites get one via serde; this constructor is for
    /// tests and for the rare in-code construction.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Read out the wrapped string. Named "expose" rather than `as_str`
    /// or `inner` so a code reviewer scanning for credential reads has
    /// a single greppable token.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// True if the wrapped string is empty. Useful in validators
    /// without exposing the secret.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `"***"` rather than `"<redacted>"` to keep the placeholder
        // shorter than any real credential — operators tailing a log
        // can spot it visually without the placeholder running into
        // surrounding fields.
        f.write_str("\"***\"")
    }
}

impl Serialize for SecretString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // We cannot drop `Serialize` entirely: the config section
        // structs (`S3Credentials`, and `FileConfig` transitively)
        // derive it for symmetry with `Deserialize`, so `SecretString`
        // must implement it for them to compile. Rather than emit the
        // cleartext, we hash it: any incidental serialization of a
        // config section — diagnostics, tests, a future call site —
        // then cannot leak the credential (defense-in-depth, the
        // `Serialize` analogue of the `Debug` redaction). Distinct
        // secrets almost always map to distinct hashes, so the form
        // stays useful for equality checks. `DefaultHasher` is
        // non-cryptographic; std documents it as producing the same
        // output across all `DefaultHasher` instances in a build, so
        // the digest is stable within a process. Cryptographic strength
        // is unnecessary — the output only guards against accidental
        // cleartext exposure, and the single-pair collision probability
        // (~2⁻⁶⁴ for a 64-bit output) is negligible.
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut h);
        // 16 hex chars covers the full u64 hash output. Prefix is
        // `hash:` (not `sha:`): the digest comes from `DefaultHasher`
        // (SipHash-1-3 in current std), not any SHA family — naming
        // it after the algorithm we don't use would be misleading.
        serializer.serialize_str(&format!("hash:{:016x}", h.finish()))
    }
}

// PartialEq is provided to keep tests ergonomic. Comparing two
// SecretStrings for equality does not leak: the comparison runs in
// constant-ish time at the byte level and produces only a `bool`.
// This is intentionally not constant-time crypto: we're guarding
// against incidental leakage via Debug/Serialize, not against side-
// channel attackers.
impl PartialEq for SecretString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SecretString {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_value() {
        let s = SecretString::new("hunter2");
        let dbg = format!("{s:?}");
        assert_eq!(dbg, "\"***\"");
        assert!(
            !dbg.contains("hunter2"),
            "raw value must never appear in Debug output"
        );
    }

    #[test]
    fn debug_redacts_inside_struct() {
        // Realistic case: a containing struct derives Debug and prints
        // every field. The wrapper must redact in that context too.
        #[derive(Debug)]
        struct Wrap {
            #[allow(dead_code)] // Read by Debug only.
            secret: SecretString,
        }
        let w = Wrap {
            secret: SecretString::new("super-secret-token-abc123"),
        };
        let dbg = format!("{w:?}");
        assert!(!dbg.contains("super-secret"), "got: {dbg}");
        assert!(dbg.contains("***"), "redaction marker missing: {dbg}");
    }

    #[test]
    fn expose_returns_raw_value() {
        let s = SecretString::new("hunter2");
        assert_eq!(s.expose(), "hunter2");
    }

    #[test]
    fn is_empty_does_not_leak_value() {
        assert!(SecretString::new("").is_empty());
        assert!(!SecretString::new("x").is_empty());
    }

    #[test]
    fn deserializes_from_toml_string() {
        // Realistic shape: secrets land via TOML -> serde into the
        // wrapper without ceremony at the call site.
        #[derive(Deserialize)]
        struct Wrap {
            secret: SecretString,
        }
        let w: Wrap = toml::from_str("secret = \"hunter2\"").unwrap();
        assert_eq!(w.secret.expose(), "hunter2");
    }

    #[test]
    fn serialize_does_not_leak_value() {
        // The serialized form must never contain the cleartext secret.
        // This is the defense-in-depth contract: any serialization of a
        // config section (the structs derive `Serialize`) must redact
        // the credential rather than surface it.
        let s = SecretString::new("hunter2-this-is-the-secret");
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            !json.contains("hunter2"),
            "raw secret leaked through Serialize: {json}"
        );
        assert!(
            json.starts_with("\"hash:") && json.ends_with('"'),
            "expected hashed form, got: {json}"
        );
    }

    #[test]
    fn serialize_distinguishes_distinct_secrets() {
        // Distinct secrets must produce distinct serialized forms so the
        // redacted output stays useful for equality/change checks rather
        // than collapsing every credential to one indistinguishable
        // token.
        let a = SecretString::new("old-key");
        let b = SecretString::new("new-key");
        let ja = serde_json::to_string(&a).unwrap();
        let jb = serde_json::to_string(&b).unwrap();
        assert_ne!(ja, jb, "distinct secrets must serialize differently");
    }

    #[test]
    fn serialize_is_deterministic_within_process() {
        // The same secret, serialized twice, must produce the same
        // bytes — a stable redacted form, so the hash is usable for
        // equality checks rather than varying run-to-run.
        let s = SecretString::new("a-stable-key");
        let first = serde_json::to_string(&s).unwrap();
        let second = serde_json::to_string(&s).unwrap();
        assert_eq!(first, second);
    }
}
