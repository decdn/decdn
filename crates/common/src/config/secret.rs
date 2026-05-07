//! [`SecretString`] — a `String` newtype with redacted [`Debug`].
//!
//! Used by [`super::types::S3Credentials::Static`] to hold AWS access
//! keys, secret keys, and session tokens. The wrapper exists so that an
//! incidental `tracing::debug!(?cfg)`, panic backtrace formatter, or
//! stray `dbg!()` cannot leak the credential into logs. The codebase
//! already has redaction discipline for HTTP-origin URL credentials
//! (see `decdn_cache::redact_for_log`) — this is the same pattern for
//! TOML-borne secrets.
//!
//! - [`std::fmt::Debug`] always prints `"***"` regardless of the
//!   wrapped value.
//! - `std::fmt::Display` is **not** implemented — use
//!   [`SecretString::expose`] when you need the raw value, so every
//!   call site is greppable.
//! - [`serde::Deserialize`] is implemented (so TOML still works).
//! - [`serde::Serialize`] is implemented but emits a redacted, hashed
//!   form (`"sha:{hash}"`) — never the cleartext secret. The hash is
//!   `DefaultHasher` (per-process keyed, not cryptographic). The
//!   purpose is to preserve SIGHUP reload diff-detection in
//!   `runtime::reload::FileSectionSnapshot`: two snapshots compare
//!   equal iff the underlying secrets are equal, without the
//!   serialized form revealing either secret.

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
    pub fn is_empty(&self) -> bool {
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
        // We cannot drop `Serialize` entirely: `runtime::reload`'s
        // `FileSectionSnapshot::capture` calls `serde_json::to_value`
        // on every config section to detect SIGHUP-time changes that
        // are non-hot-reloadable. If we skipped the credentials
        // field, an operator rotating the secret would see "no
        // change" instead of "cache.* requires restart". So we emit
        // a deterministic-per-process hash: distinct secrets map to
        // distinct hashes (preserving diff detection) without the
        // serialized form revealing the secret. The serialized
        // value is only ever held in-memory or compared structurally
        // in `runtime::reload`; it is never logged or written to a
        // file by anything in this codebase.
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut h);
        // 16 hex chars covers the full u64 hash output.
        serializer.serialize_str(&format!("sha:{:016x}", h.finish()))
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
        // The serialized form must never contain the cleartext
        // secret. This is the contract `runtime::reload::snap_section`
        // depends on — it serializes the entire FileConfig section
        // and any leak would surface in the in-memory snapshot Value.
        let s = SecretString::new("hunter2-this-is-the-secret");
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            !json.contains("hunter2"),
            "raw secret leaked through Serialize: {json}"
        );
        assert!(
            json.starts_with("\"sha:") && json.ends_with('"'),
            "expected hashed form, got: {json}"
        );
    }

    #[test]
    fn serialize_distinguishes_distinct_secrets() {
        // Diff detection in `runtime::reload::FileSectionSnapshot`
        // compares serialized forms structurally. Distinct secrets
        // must produce distinct serialized forms; otherwise an
        // operator rotating credentials via SIGHUP would see "no
        // change" and not get the "cache.* requires restart"
        // warning.
        let a = SecretString::new("old-key");
        let b = SecretString::new("new-key");
        let ja = serde_json::to_string(&a).unwrap();
        let jb = serde_json::to_string(&b).unwrap();
        assert_ne!(ja, jb, "distinct secrets must serialize differently");
    }

    #[test]
    fn serialize_is_deterministic_within_process() {
        // The same secret, serialized twice, must produce the same
        // bytes — otherwise `snap_section` would always see a
        // "change" between reloads and emit spurious "requires
        // restart" warnings.
        let s = SecretString::new("a-stable-key");
        let first = serde_json::to_string(&s).unwrap();
        let second = serde_json::to_string(&s).unwrap();
        assert_eq!(first, second);
    }
}
