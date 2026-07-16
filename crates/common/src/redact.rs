//! Credential redaction for URL-ish strings shared across the binaries.
//!
//! Two related invariants live here, both keyed on the rule that a URL can
//! carry a secret no log or operator-facing error may echo:
//!
//! - **Userinfo** — relay/origin URLs can carry `user:pass@` userinfo. When
//!   such a string is named in a parse error or log (config-validate time,
//!   [`crate::config`]; node bring-up, the `decdn-node` runtime), the
//!   credentials are scrubbed by [`redact_userinfo`], which preserves the host
//!   and path so the offending entry is still identifiable.
//! - **Whole URL** — a chain `rpc_url` secret commonly lives in the path or
//!   query (Infura/Alchemy keys), which userinfo redaction would NOT scrub. So
//!   when an `rpc_url` reaches a *transport/chain error*, [`strip_urls`] (via
//!   [`sanitize_rpc_display`] / [`sanitize_err_chain`]) removes the URL
//!   entirely, keeping only the failure class.
//!
//! Routing every gate through this single module keeps each invariant from
//! drifting between copies.

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

/// Strip URL-bearing fragments from an error's rendered text so an `rpc_url`
/// secret never reaches a log or operator-facing error — the transport-failure
/// *class* (timeout / connection refused / DNS / TLS) still shows.
///
/// Unlike [`redact_userinfo`], which only scrubs `user:pass@` userinfo, this
/// removes the whole URL: an `rpc_url` secret commonly lives in the path or
/// query (Infura/Alchemy keys), matching `config validate`'s policy of never
/// echoing `rpc_url`. Two fragment shapes are scrubbed:
///
/// - `reqwest`'s Display tail ` for url (<url>)` (reqwest 0.13.x; the inner
///   `write!(f, " for url ({url})")`), which `alloy`'s `#[error("{0}")]`-
///   transparent transport error forwards verbatim — the entire ` for url (...)`
///   clause is dropped; and
/// - any remaining bare `<scheme>://` token, replaced with `<redacted-url>` as
///   a backstop for non-`reqwest` renderings.
///
/// Text with no URL scheme passes through borrowed (no copy).
pub fn strip_urls(raw: &str) -> Cow<'_, str> {
    match remove_for_url_clauses(raw) {
        // Nothing dropped: feed the original slice on so the borrow survives.
        Cow::Borrowed(s) => redact_bare_urls(s),
        // A clause was dropped. Only re-scan when a bare URL might remain;
        // otherwise hand back the owned buffer without a redundant copy.
        Cow::Owned(s) if s.contains("://") => Cow::Owned(redact_bare_urls(&s).into_owned()),
        Cow::Owned(s) => Cow::Owned(s),
    }
}

/// Drop every ` for url (...)` clause (reqwest's Display tail) from `s`.
fn remove_for_url_clauses(s: &str) -> Cow<'_, str> {
    const NEEDLE: &str = " for url (";
    if !s.contains(NEEDLE) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find(NEEDLE) {
        out.push_str(rest.get(..pos).unwrap_or(""));
        // reqwest writes ` for url ({url})`; a URL contains no whitespace, so
        // scan the token to the next whitespace/end rather than the first `)` —
        // a `)` can appear unescaped in a URL path/query (`url::Url` leaves
        // sub-delims as-is), and stopping there would strand the secret-bearing
        // tail. A single trailing `)` (the clause's own close) is then dropped.
        let after = rest.get(pos + NEEDLE.len()..).unwrap_or("");
        let url_end = after
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after.len());
        let tail = after.get(url_end..).unwrap_or("");
        rest = tail.strip_prefix(')').unwrap_or(tail);
    }
    out.push_str(rest);
    Cow::Owned(out)
}

/// Replace each bare `<scheme>://<token>` in `s` with `<redacted-url>`.
///
/// Keys off the `://` separator and walks back over the scheme characters
/// (RFC 3986: ALPHA / DIGIT / `+` / `-` / `.`), so it matches ANY scheme
/// regardless of case — `http`, `HTTPS`, `ws`, `git+ssh`, … — rather than a
/// hard-coded `http`/`https` prefix a future non-`reqwest` renderer (or an
/// uppercased URL) could slip past.
fn redact_bare_urls(s: &str) -> Cow<'_, str> {
    if !s.contains("://") {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(sep) = rest.find("://") {
        // Walk back over the scheme characters to the URL's start.
        let before = rest.get(..sep).unwrap_or("");
        let scheme_len = before
            .bytes()
            .rev()
            .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
            .count();
        let url_start = sep - scheme_len;
        out.push_str(rest.get(..url_start).unwrap_or(""));
        out.push_str("<redacted-url>");
        // A URL contains no whitespace; consume the whole token. `)`/`?`/`&`
        // etc. can be part of the path/query, so only whitespace ends it —
        // over-consuming adjacent punctuation is safe; leaking a secret is not.
        let after = rest.get(sep + 3..).unwrap_or("");
        let end = after
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after.len());
        rest = after.get(end..).unwrap_or("");
    }
    out.push_str(rest);
    Cow::Owned(out)
}

/// Render any error's Display, with URL fragments stripped, for a single
/// log/error line. Use at `tracing` sites where the value is the raw `alloy`
/// transport error (its transparent Display carries the `reqwest` URL tail).
///
/// Prefer [`sanitize_err_chain`] for an `anyhow::Error`: Display renders only
/// the *outermost* context, so any `.with_context(…)` on the way up silently
/// replaces the underlying reason rather than adding to it.
pub fn sanitize_rpc_display(err: impl std::fmt::Display) -> String {
    strip_urls(&err.to_string()).into_owned()
}

/// Render an `anyhow` error's full `context: cause: cause` chain (alternate
/// Display) with URL fragments stripped.
///
/// Use for any propagated chain-RPC error — at the `main()` print boundary so
/// no raw `rpc_url` is echoed, and at watcher `tracing` sites so the reason
/// survives. The latter is not a style preference: a chain read is wrapped by
/// `chain_events::timed`, whose "… timed out after 10s" is the *cause*, and
/// call sites add context above it (`getOrigins(namespace=1)`,
/// `nodeIdOf(0x…)`). Rendering such an error with plain Display prints only
/// that context and drops the timeout entirely — the operator sees a bare
/// restatement of what was attempted, never why it failed.
pub fn sanitize_err_chain(err: &anyhow::Error) -> String {
    strip_urls(&format!("{err:#}")).into_owned()
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

    #[test]
    fn strip_urls_drops_reqwest_for_url_tail() {
        // The `reqwest` Display tail (forwarded by alloy) carries the secret in
        // its path; the whole ` for url (...)` clause must go, keeping the class.
        let raw = "error sending request for url (https://eth.example/v3/SECRETKEY)";
        let cleaned = strip_urls(raw);
        assert_eq!(cleaned, "error sending request");
        assert!(!cleaned.contains("SECRETKEY"));
        assert!(!cleaned.contains("eth.example"));
    }

    #[test]
    fn strip_urls_redacts_bare_url_token() {
        // A URL not in the ` for url (...)` form is still scrubbed, leaving the
        // surrounding text intact.
        let raw = "failed to reach https://eth.example/v3/SECRETKEY?k=1 after 3 tries";
        let cleaned = strip_urls(raw);
        assert_eq!(cleaned, "failed to reach <redacted-url> after 3 tries");
        assert!(!cleaned.contains("SECRETKEY"));
    }

    #[test]
    fn strip_urls_handles_both_shapes_and_trailing_delimiters() {
        let raw = "ctx (https://a.example/KEY): sending request for url (http://b.example/KEY2)";
        let cleaned = strip_urls(raw);
        assert!(!cleaned.contains("KEY"));
        assert!(!cleaned.contains("a.example"));
        assert!(!cleaned.contains("b.example"));
        assert!(cleaned.contains("<redacted-url>"));
        assert!(cleaned.contains("sending request"));
    }

    #[test]
    fn strip_urls_passes_clean_text_through_borrowed() {
        // No URL scheme => no copy, value verbatim.
        assert!(matches!(
            strip_urls("connection refused (os error 111)"),
            Cow::Borrowed("connection refused (os error 111)")
        ));
    }

    #[test]
    fn strip_urls_redacts_any_scheme_and_case() {
        // The bare-URL backstop keys off `://`, so an uppercased or non-http
        // scheme (a future non-reqwest renderer could emit either) is covered.
        for raw in [
            "x HTTPS://ETH.EXAMPLE/V3/SECRETKEY y",
            "x Https://eth.example/SECRETKEY y",
            "x ws://eth.example/SECRETKEY y",
        ] {
            let cleaned = strip_urls(raw);
            assert!(!cleaned.contains("SECRETKEY"), "leaked: {cleaned}");
            assert_eq!(cleaned, "x <redacted-url> y");
        }
    }

    #[test]
    fn strip_urls_redacts_query_only_and_userinfo_secrets() {
        // Secret in the query string (not the path) is still consumed...
        let q = strip_urls("reach https://eth.example/rpc?apikey=SECRETKEY now");
        assert!(!q.contains("SECRETKEY"));
        assert_eq!(q, "reach <redacted-url> now");
        // ...and a userinfo-bearing URL is removed WHOLE (unlike redact_userinfo,
        // which would keep the host/path).
        let u = strip_urls("reach https://user:pass@eth.example/v3/SECRETKEY now");
        assert!(!u.contains("SECRETKEY") && !u.contains("eth.example"));
        assert_eq!(u, "reach <redacted-url> now");
    }

    #[test]
    fn strip_urls_handles_url_only_and_multiple_clauses() {
        // A message that is nothing but a URL collapses to the marker.
        assert_eq!(
            strip_urls("https://eth.example/v3/SECRETKEY"),
            "<redacted-url>"
        );
        // Two `for url (...)` clauses are both dropped.
        let two =
            strip_urls("boom for url (https://a.example/K1) then for url (https://b.example/K2)");
        assert!(!two.contains("K1") && !two.contains("K2"));
        assert_eq!(two, "boom then");
    }

    #[test]
    fn strip_urls_strips_url_containing_literal_paren() {
        // A `)` is a legal unescaped sub-delim in a URL path/query; it must not
        // end the ` for url (...)` clause early and strand the secret tail.
        let clause = strip_urls("read failed for url (https://eth.example/pa)th?key=SECRETKEY)");
        assert!(!clause.contains("SECRETKEY"), "leaked: {clause}");
        assert_eq!(clause, "read failed");
        // Same for the bare-URL backstop (no ` for url (` wrapper).
        let bare = strip_urls("reach https://eth.example/pa)th?key=SECRETKEY now");
        assert!(!bare.contains("SECRETKEY"), "leaked: {bare}");
        assert_eq!(bare, "reach <redacted-url> now");
    }

    #[test]
    fn strip_urls_drops_unterminated_for_url_tail() {
        // A truncated/garbled tail with no closing paren drops everything after
        // the needle rather than leaking the partial URL.
        let cleaned = strip_urls("read failed for url (https://eth.example/SECRETKEY");
        assert!(!cleaned.contains("SECRETKEY"));
        assert_eq!(cleaned, "read failed");
    }

    #[test]
    fn strip_urls_is_utf8_safe_around_multibyte_text() {
        // Multibyte content adjacent to the URL must not panic on a byte index.
        let cleaned = strip_urls("café https://eth.example/v3/SECRETKEY ☕ done");
        assert!(!cleaned.contains("SECRETKEY"));
        assert_eq!(cleaned, "café <redacted-url> ☕ done");
    }

    #[test]
    fn sanitize_err_chain_strips_url_across_context_layers() {
        let inner =
            anyhow::anyhow!("error sending request for url (https://eth.example/v3/SECRETKEY)");
        let chained = inner.context("failed to read minCapacityMbps (RPC reachable?)");
        let out = sanitize_err_chain(&chained);
        assert!(out.contains("failed to read minCapacityMbps"));
        assert!(!out.contains("SECRETKEY"));
        assert!(!out.contains("eth.example"));
    }

    #[test]
    fn sanitize_rpc_display_strips_url_from_plain_error() {
        let err =
            std::io::Error::other("request for url (https://eth.example/v3/SECRETKEY) timed out");
        let out = sanitize_rpc_display(&err);
        assert!(!out.contains("SECRETKEY"));
        assert!(out.contains("timed out"));
    }
}
