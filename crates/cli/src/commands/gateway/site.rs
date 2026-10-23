//! Site origins (#2457): a bundle served as a website on its own browser
//! origin, `http://[<ns>.]<hash32>.localhost:<port>/`.
//!
//! `<hash32>` is the manifest hash in RFC 4648 base32, lowercase and
//! unpadded: 52 characters, inside the 63-character DNS label limit that the
//! 64-character hex form exceeds. `<ns>` is the namespace routing hint in
//! decimal. Browsers resolve every `localhost` name to loopback, so each
//! bundle is its own origin, with its own storage, cookies and scripts. The
//! namespace sits left of the hash, so the registrable domain of a site
//! (`<hash32>.localhost`) is its bundle's, and no page can claim a scope over
//! other bundles.
//!
//! A site origin takes no gateway token: the browser's `Sec-Fetch-*` headers,
//! which page scripts cannot set, decide what a request may spend.

use std::net::SocketAddr;

use data_encoding::BASE32_NOPAD;
use hyper::{HeaderMap, StatusCode};

/// The domain every site origin sits under.
const SITE_DOMAIN: &str = "localhost";

/// A `<hash32>` label's length.
const LABEL_LEN: usize = 52;

/// A bundle served as a website, by its manifest hash and namespace hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Site {
    pub(super) hash: [u8; 32],
    pub(super) namespace: Option<u64>,
}

impl Site {
    /// The site's host name, `[<ns>.]<hash32>.localhost`.
    pub(super) fn host(&self) -> String {
        let label = BASE32_NOPAD.encode(&self.hash).to_ascii_lowercase();
        match self.namespace {
            Some(ns) => format!("{ns}.{label}.{SITE_DOMAIN}"),
            None => format!("{label}.{SITE_DOMAIN}"),
        }
    }

    /// The site a `Host` header names on the gateway's `port`, or `None` when
    /// it names no well-formed site.
    pub(super) fn from_host(host: &str, port: u16) -> Option<Self> {
        let name = host
            .strip_suffix(port.to_string().as_str())?
            .strip_suffix(':')?;
        let rest = name.strip_suffix(SITE_DOMAIN)?.strip_suffix('.')?;
        let (namespace, label) = match rest.split_once('.') {
            Some((ns, label)) => (Some(parse_namespace(ns)?), label),
            None => (None, rest),
        };
        Some(Self {
            hash: decode_label(label)?,
            namespace,
        })
    }
}

/// The hash a canonical `<hash32>` label encodes.
fn decode_label(label: &str) -> Option<[u8; 32]> {
    if label.len() != LABEL_LEN
        || !label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
    {
        return None;
    }
    // The decoder refuses nonzero trailing bits, so each hash has one label.
    BASE32_NOPAD
        .decode(label.to_ascii_uppercase().as_bytes())
        .ok()?
        .try_into()
        .ok()
}

/// A canonical decimal namespace id: no leading zero, never `0`.
fn parse_namespace(s: &str) -> Option<u64> {
    if s.starts_with('0') || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Whether a request may spend on a site origin: the token holder always;
/// in a browser, the site's own requests (`same-origin`), the user's own
/// (`none`: a typed URL, a bookmark), and a top-level navigation from
/// anywhere, so a link opens a site at any path. Every other cross-origin
/// request is refused, so no other page can make the gateway pay. A request
/// without `Sec-Fetch-Site` comes from outside a browser and needs the token.
pub(super) fn may_spend(
    headers: &HeaderMap,
    authorized: bool,
) -> Result<(), (StatusCode, &'static str)> {
    if authorized {
        return Ok(());
    }
    let Some(fetch_site) = header(headers, "sec-fetch-site") else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "token required outside a browser\n",
        ));
    };
    let top_level = is_navigation(headers) && header(headers, "sec-fetch-dest") == Some("document");
    if matches!(fetch_site, "same-origin" | "none") || top_level {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "a site answers only its own pages and top-level navigation\n",
        ))
    }
}

/// Whether the request navigates a browsing context: a link, a form, an
/// iframe.
pub(super) fn is_navigation(headers: &HeaderMap) -> bool {
    header(headers, "sec-fetch-mode") == Some("navigate")
}

/// The policy every site response carries: a page loads only from its own
/// origin, plus the bare gateway origin for `/_delegate` and `/_capability`,
/// so it reaches no content outside what the gateway serves it. Inline code
/// comes from the same bundle as the page, so it runs.
pub(super) fn content_security_policy(listen: SocketAddr) -> String {
    format!(
        "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:; \
         connect-src 'self' http://{listen}"
    )
}

/// The `Content-Type` for an entry on a site origin: the type its name
/// gives, text in UTF-8, and bytes for a name with no known extension.
pub(super) fn content_type(name: &str) -> String {
    let guess = mime_guess::from_path(name).first_or_octet_stream();
    if guess.type_() == mime_guess::mime::TEXT {
        format!("{}; charset=utf-8", guess.essence_str())
    } else {
        guess.essence_str().to_owned()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
