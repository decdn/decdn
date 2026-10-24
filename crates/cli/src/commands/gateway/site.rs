//! Site origins (#2457): a bundle served as a website on its own browser
//! origin, `http://[<ns>.]<hash32>.localhost:<port>/` on a loopback gateway
//! and `<scheme>://[<ns>.]<hash32>.<domain>/` on a public one
//! ([`SiteDomain`]).
//!
//! `<hash32>` is the manifest hash in RFC 4648 base32, lowercase and
//! unpadded: 52 characters, inside the 63-character DNS label limit that the
//! 64-character hex form exceeds. `<ns>` is the namespace routing hint in
//! decimal. Browsers resolve every `localhost` name to loopback, and a public
//! domain's wildcard record resolves every label, so each bundle is its own
//! origin, with its own storage and scripts. The namespace sits left of the
//! hash, so the registrable domain of a site (`<hash32>.localhost`) is its
//! bundle's, and no page can claim a scope over other bundles. Cookies and
//! other site-scoped state are separate per bundle only where the suffix is
//! a public suffix, as `localhost` is: a public domain's operator registers
//! it on the Public Suffix List, as `github.io` is. On a public domain a
//! namespace alone, `<ns>.<domain>`, names the bundle the namespace's pointer
//! gives.
//!
//! A site origin takes no gateway token: the browser's `Sec-Fetch-*` headers,
//! which page scripts cannot set, decide what a request may spend.

use std::net::SocketAddr;

use data_encoding::BASE32_NOPAD;
use hyper::{HeaderMap, StatusCode};
use url::Url;

/// A `<hash32>` label's length.
const LABEL_LEN: usize = 52;

/// Where site origins live: under `localhost` on the gateway's loopback port,
/// or under a public origin's host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SiteDomain {
    /// `http://[<ns>.]<hash32>.localhost:<port>`, the loopback gateway at
    /// `listen`.
    Loopback {
        /// The gateway's own address, and the port of every site.
        listen: SocketAddr,
    },
    /// `<scheme>://[<ns>.]<hash32>.<host>[:<port>]` and
    /// `<scheme>://<ns>.<host>[:<port>]` under the public `origin`.
    Public {
        /// The gateway's public origin, `https://decdn.link`.
        origin: Url,
    },
}

/// What a `Host` names under a [`SiteDomain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Route {
    /// The public gateway's own origin.
    Bare,
    /// A bundle on its own origin.
    Site(Site),
    /// A namespace on its own origin, serving the bundle its pointer names.
    Namespace(u64),
}

impl SiteDomain {
    /// What every host under the domain ends with: `localhost:<port>`, or
    /// the public origin's `host[:port]`.
    fn suffix(&self) -> String {
        match self {
            Self::Loopback { listen } => format!("localhost:{}", listen.port()),
            Self::Public { origin } => match (origin.host_str(), origin.port()) {
                (Some(host), Some(port)) => format!("{host}:{port}"),
                (Some(host), None) => host.to_owned(),
                (None, _) => String::new(),
            },
        }
    }

    /// `site`'s host, `[<ns>.]<hash32>.<suffix>`.
    pub(super) fn site_host(&self, site: &Site) -> String {
        let label = BASE32_NOPAD.encode(&site.hash).to_ascii_lowercase();
        let suffix = self.suffix();
        match site.namespace {
            Some(ns) => format!("{ns}.{label}.{suffix}"),
            None => format!("{label}.{suffix}"),
        }
    }

    /// The origin of `host`, a host under this domain: the scheme and, on
    /// loopback, the port.
    pub(super) fn origin(&self, host: &str) -> String {
        match self {
            Self::Loopback { listen } => {
                if host.contains(':') {
                    format!("http://{host}")
                } else {
                    format!("http://{host}:{}", listen.port())
                }
            }
            Self::Public { origin } => format!("{}://{host}", origin.scheme()),
        }
    }

    /// What `host` names under this domain, or `None` for a host the domain
    /// does not answer. On loopback the gateway's own names are the caller's
    /// to allow, and a namespace alone names nothing.
    pub(super) fn route(&self, host: &str) -> Option<Route> {
        let suffix = self.suffix();
        if suffix.is_empty() {
            return None;
        }
        let public = matches!(self, Self::Public { .. });
        if public && host == suffix {
            return Some(Route::Bare);
        }
        let labels = host.strip_suffix(suffix.as_str())?.strip_suffix('.')?;
        if let Some(hash) = decode_label(labels) {
            return Some(Route::Site(Site {
                hash,
                namespace: None,
            }));
        }
        if let Some((ns, label)) = labels.split_once('.') {
            return Some(Route::Site(Site {
                hash: decode_label(label)?,
                namespace: Some(parse_namespace(ns)?),
            }));
        }
        public
            .then(|| parse_namespace(labels))
            .flatten()
            .map(Route::Namespace)
    }
}

/// A bundle served as a website, by its manifest hash and namespace hint.
/// Its host is [`SiteDomain::site_host`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Site {
    pub(super) hash: [u8; 32],
    pub(super) namespace: Option<u64>,
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
/// iframe. A request with no `Sec-Fetch-Mode` comes from outside a browser,
/// or from one that sends none, and is taken as a navigation: another
/// bundle's pages then run on their own origin, never on this one.
pub(super) fn is_navigation(headers: &HeaderMap) -> bool {
    header(headers, "sec-fetch-mode").is_none_or(|mode| mode == "navigate")
}

/// The policy every site response carries: a page loads only from its own
/// origin, plus on loopback the bare gateway origin for `/_delegate` and
/// `/_capability`, so it reaches no content outside what the gateway serves
/// it. A public gateway has no API a page may call. Inline code comes from
/// the same bundle as the page, so it runs.
pub(super) fn content_security_policy(domain: &SiteDomain) -> String {
    let connect = match domain {
        SiteDomain::Loopback { listen } => format!(" http://{listen}"),
        SiteDomain::Public { .. } => String::new(),
    };
    format!(
        "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:; \
         connect-src 'self'{connect}"
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
