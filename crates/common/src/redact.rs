//! Credential redaction for URL-ish strings shared across the binaries.
//!
//! Relay/origin URLs can carry `user:pass@` userinfo. Whenever such a string
//! is echoed into an error or log — at config-validate time
//! ([`crate::config`]) or at node bring-up (the `decdn-node` runtime) — the
//! credentials must be scrubbed first. Both gates call the single
//! [`redact_userinfo`] here so the invariant ("relay userinfo never reaches a
//! log or operator-facing error") cannot drift between two copies.

use std::borrow::Cow;

/// Replace any `user:pass@` userinfo in a URL-ish string with `***@` so a
/// parse-failure log/error can name the offending entry without leaking
/// credentials.
///
/// The input may be an *already-malformed* entry, so we can't lean on a URL
/// parser. The authority begins after the first `://` (a scheme separator) or
/// at the start of the string when there is none — a typo'd entry may drop the
/// scheme yet still carry credentials. Within the authority (up to the host's
/// first `/`, `?`, or `#`), userinfo runs to the *last* `@`, so a password
/// containing a literal `@` is fully redacted, while an `@` in the path/query/
/// fragment is left intact. Strings with no authority `@` pass through
/// unchanged (borrowed, no copy), so non-credential values still appear
/// verbatim in errors.
pub fn redact_userinfo(raw: &str) -> Cow<'_, str> {
    let authority_start = raw.find("://").map_or(0, |i| i + 3);
    let after = raw.get(authority_start..).unwrap_or("");
    let host_delim = after.find(['/', '?', '#']).unwrap_or(after.len());
    match after.get(..host_delim).and_then(|a| a.rfind('@')) {
        Some(at) => {
            let prefix = raw.get(..authority_start).unwrap_or("");
            let rest = after.get(at + 1..).unwrap_or("");
            Cow::Owned(format!("{prefix}***@{rest}"))
        }
        None => Cow::Borrowed(raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_userinfo_redacts_all_credential_shapes() {
        // Credentialed authority is redacted; host + path/query are preserved.
        assert_eq!(
            redact_userinfo("https://user:pass@host:7842/path"),
            "https://***@host:7842/path"
        );
        // Password containing a literal `@` must be fully redacted (last-`@`).
        assert_eq!(
            redact_userinfo("https://user:p@ss@host:notaport"),
            "https://***@host:notaport"
        );
        // Credentials with no scheme separator still get redacted.
        assert_eq!(redact_userinfo("relay user:s3cret@host"), "***@host");
        assert_eq!(redact_userinfo("user@pass:s3cret@host"), "***@host");
        // Empty userinfo and IPv6 host after userinfo.
        assert_eq!(redact_userinfo("https://@host"), "https://***@host");
        assert_eq!(
            redact_userinfo("https://user@[::1]:443"),
            "https://***@[::1]:443"
        );
        // No userinfo => unchanged (borrowed, not copied).
        assert!(matches!(
            redact_userinfo("https://relay.example:7842"),
            Cow::Borrowed("https://relay.example:7842")
        ));
        assert!(matches!(
            redact_userinfo("not a url"),
            Cow::Borrowed("not a url")
        ));
        // An `@` in the path (after the host) is not userinfo — leave it.
        assert_eq!(
            redact_userinfo("https://host/path@x"),
            "https://host/path@x"
        );
    }
}
