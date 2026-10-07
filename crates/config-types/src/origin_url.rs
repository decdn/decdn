//! Parsed, scheme-validated origin base URL.
//!
//! Wraps `url::Url` (not `reqwest::Url`) so this leaf crate does not
//! pull `reqwest`. `reqwest::Url` is a re-export of `url::Url`, so the
//! type is identical at the cache's HTTP-origin call sites.

use anyhow::Context;
use url::Url;

/// Return a loggable form of an origin URL with userinfo (`user:pass@`)
/// stripped. Operators may legitimately use basic-auth-in-URL for
/// internal origins, but those credentials must not reach logs or
/// error messages.
///
/// Fails closed: if the setters can't strip userinfo (cannot-be-a-base
/// URLs, or URL-parsing ambiguity that misattributes credentials into
/// host/path), we emit a fixed placeholder rather than echoing anything
/// that might contain secrets. Callers from `parse_origin_url` and the
/// HTTP origin's fetch path always pass http/https URLs (parse-and-scheme
/// validated, with `.join()` preserving scheme), so the happy path is
/// expected here.
#[must_use]
pub fn redact_for_log(url: &Url) -> String {
    let mut u = url.clone();
    let _ = u.set_username("");
    let _ = u.set_password(None);
    if !u.username().is_empty() || u.password().is_some() {
        return "<redacted URL>".to_string();
    }
    u.to_string()
}

/// Loggable form of a raw (not yet parsed) origin URL string. Used by
/// `parse_origin_url` when the input might contain credentials. For
/// unparseable input we don't echo it at all — no way to safely split
/// userinfo from the rest.
fn redact_raw_for_log(raw: &str) -> String {
    match Url::parse(raw) {
        Ok(url) => redact_for_log(&url),
        Err(_) => "<unparseable URL>".to_string(),
    }
}

/// Parsed, scheme-validated, and path-normalized origin base URL. The
/// only way to construct one is [`parse_origin_url`] — once you have an
/// [`OriginUrl`], the following invariants are guaranteed by the type:
///
/// 1. Scheme is `http` or `https`.
/// 2. Path ends with a trailing `/`, so `{base}.join(&hex)` produces
///    `{base}/{hex}` rather than overwriting the final path component.
/// 3. No query string or fragment — both silently get dropped by
///    `Url::join` and would turn into invisible footguns.
#[derive(Debug, Clone)]
pub struct OriginUrl(Url);

impl OriginUrl {
    /// Borrow the underlying [`url::Url`]. Needed when calling
    /// `.join(hash_hex)` or handing the URL to `reqwest::Client::get`.
    #[must_use]
    pub const fn as_url(&self) -> &Url {
        &self.0
    }

    /// Consume into the underlying [`url::Url`].
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Destructuring a non-Copy newtype is not const.
    pub fn into_url(self) -> Url {
        self.0
    }
}

impl std::fmt::Display for OriginUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Parse and normalize an origin base URL. This is the single source of
/// truth for origin-URL validation — config resolution and test helpers
/// both go through it, and the returned [`OriginUrl`] carries the
/// scheme/path/query invariants in the type.
pub fn parse_origin_url(raw: &str) -> anyhow::Result<OriginUrl> {
    // Error messages use `redact_raw_for_log` / `redact_for_log` so
    // userinfo (user:pass@) never reaches error strings or logs.
    let mut url = Url::parse(raw)
        .with_context(|| format!("invalid origin base URL: {}", redact_raw_for_log(raw)))?;
    match url.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!(
            "unsupported origin URL scheme {other:?} (expected http or https): {}",
            redact_for_log(&url)
        ),
    }
    // A query or fragment on the base would be silently dropped by
    // `Url::join` when we append the blake3 hex, so reject up front rather
    // than accept a URL that can't possibly do what the operator expects.
    if url.query().is_some() {
        anyhow::bail!(
            "origin URL must not contain a query string: {}",
            redact_for_log(&url)
        );
    }
    if url.fragment().is_some() {
        anyhow::bail!(
            "origin URL must not contain a fragment: {}",
            redact_for_log(&url)
        );
    }
    // Normalize the path (not the raw string) so `http://host/v1` becomes
    // `http://host/v1/` without corrupting any other URL component.
    if !url.path().ends_with('/') {
        let normalized = format!("{}/", url.path());
        url.set_path(&normalized);
    }
    Ok(OriginUrl(url))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
