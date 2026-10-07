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
//!   when an `rpc_url` reaches a *transport/chain error*, [`strip_urls`]
//!   removes the URL entirely. The failure class (timeout, connection refused,
//!   DNS, TLS) usually lives in the error's `source()` chain, not in its
//!   top-level Display. [`sanitize_err_chain`] (for an `anyhow::Error`) and
//!   [`sanitize_error_sources`] (for a typed `std::error::Error`) render that
//!   chain, so the class survives the strip. [`sanitize_rpc_display`] renders
//!   only the top-level Display, so it suits a plain `Display` value such as a
//!   `String` reason.
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
/// secret never reaches a log or operator-facing error. Any failure class in
/// the input text (timeout / connection refused / DNS / TLS) is kept; to
/// include the `source()` chain, call [`sanitize_error_sources`] or
/// [`sanitize_err_chain`].
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

/// Render a plain `Display` value, with URL fragments stripped, for a single
/// log/error line. Use for a value with no `source()` chain, such as a
/// `String` reason.
///
/// For an error, prefer a helper that walks the chain: Display renders only
/// the *outermost* layer, so the failure class beneath it is lost.
/// [`sanitize_err_chain`] takes an `anyhow::Error`, whose `.with_context(…)`
/// layers would otherwise replace the underlying reason.
/// [`sanitize_error_sources`] takes a typed `std::error::Error` (an `alloy`
/// `ContractError`, `RpcError`, or `PendingTransactionError`), whose
/// transport class (timeout, connection refused, DNS, TLS) sits in `source()`.
pub fn sanitize_rpc_display(err: impl std::fmt::Display) -> String {
    strip_urls(&err.to_string()).into_owned()
}

/// Render a typed error and each layer of its `source()` chain as
/// `outer: cause: cause`, with URL fragments stripped.
///
/// Use for a typed `std::error::Error`, such as an `alloy` transport or
/// contract error. Its top-level Display often names only the operation and
/// the URL, while the failure class (timeout, connection refused, DNS, TLS)
/// is a deeper `source()`. A layer is skipped when its Display is empty, or
/// when the previous non-empty layer's raw Display ends with it at a
/// non-alphanumeric boundary: a wrapper that ends its message with its source
/// (`"{0}"`, `"transport: {0}"`) would otherwise print that source twice. A
/// cause that only appears inside an earlier message or URL (`"timeout"` under
/// `"request timeout exceeded"`) is still printed. Each layer is stripped on
/// its own before the join, so a URL token cannot swallow the `": "` separator
/// after it. For an `anyhow::Error`, use [`sanitize_err_chain`].
pub fn sanitize_error_sources(err: &(dyn std::error::Error + 'static)) -> String {
    let mut prev = err.to_string();
    let mut out = strip_urls(&prev).into_owned();
    let mut next = err.source();
    while let Some(layer) = next {
        let text = layer.to_string();
        if !text.is_empty() {
            let repeats = prev.strip_suffix(text.as_str()).is_some_and(|head| {
                head.chars()
                    .next_back()
                    .is_none_or(|c| !c.is_alphanumeric())
            });
            if !repeats {
                out.push_str(": ");
                out.push_str(&strip_urls(&text));
            }
            prev = text;
        }
        next = layer.source();
    }
    out
}

/// Render an `anyhow` error's full `context: cause: cause` chain with URL
/// fragments stripped, through [`sanitize_error_sources`]: the context layers
/// and the wrapped error's `source()` chain share one walk, so a repeated
/// layer is printed once and each layer is stripped on its own.
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
    sanitize_error_sources(err.as_ref())
}

#[cfg(test)]
mod tests;
