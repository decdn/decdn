//! The user's consent to a site a page on another site sent them to
//! (#2772). A site's pages spend the gateway's capabilities on what they
//! load, so on a loopback gateway a browser's top-level navigation to a site
//! from another site (`Sec-Fetch-Site: cross-site` or `same-site`) gets an
//! interstitial page that names the site, and the site serves nothing until
//! the user continues. A typed URL or a bookmark (`none`) and the site's own
//! navigation (`same-origin`) pass straight through, as does a client
//! outside a browser, which sends no `Sec-Fetch-Site`.
//!
//! The interstitial's Continue is a form that posts back to the page's own
//! URL with a token: a keyed BLAKE3 hash of the site's host under a key the
//! gateway draws at start. The gateway takes the post only from the site's
//! own origin (`Sec-Fetch-Site: same-origin` and an `Origin` that is the
//! site's), so no other page submits it, and the interstitial may not be
//! framed, so no other page overlays it. A consent lasts until the gateway
//! stops, and lives in memory only, as the gateway's keys do.

use std::collections::HashSet;

use hyper::{HeaderMap, StatusCode, header};

use super::site::Site;
use crate::commands::local_http::{Body, full};

/// The policy on the interstitial: no script, no subresource, its form
/// posts to its own origin only, and no page may frame it.
pub(super) const POLICY: &str =
    "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'";

/// The form field that carries the token.
const FIELD: &str = "token";

/// The sites the user continued to, and the key their tokens hash under.
pub(super) struct Consents {
    key: [u8; 32],
    given: std::sync::Mutex<HashSet<Site>>,
}

impl Default for Consents {
    fn default() -> Self {
        Self {
            key: rand::random(),
            given: std::sync::Mutex::default(),
        }
    }
}

impl Consents {
    /// Whether the user continued to `site`.
    pub(super) fn given(&self, site: &Site) -> bool {
        self.given
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(site)
    }

    /// The token the interstitial for the site at `host` posts.
    fn token(&self, host: &str) -> String {
        blake3::keyed_hash(&self.key, host.as_bytes())
            .to_hex()
            .to_string()
    }

    /// Records the user's consent to `site` at `host` when `form`, a posted
    /// `application/x-www-form-urlencoded` body, carries its token.
    pub(super) fn give(&self, site: Site, host: &str, form: &[u8]) -> bool {
        let expected = self.token(host);
        let carried = url::form_urlencoded::parse(form)
            .any(|(name, value)| name == FIELD && value == expected.as_str());
        if carried {
            self.given
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(site);
        }
        carried
    }

    /// The interstitial for `site` at `host`: what the site is, what
    /// continuing lets it do, and the Continue form.
    pub(super) fn page(&self, site: &Site, host: &str) -> hyper::Response<Body> {
        let hash = alloy::hex::encode(site.hash);
        let namespace = site
            .namespace
            .map(|ns| format!("<dt>Namespace</dt><dd><code>{ns}</code></dd>"))
            .unwrap_or_default();
        let token = self.token(host);
        let page = format!(
            "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
             content=\"width=device-width\"><title>Open this decdn site?</title><style>\
             body{{font-family:system-ui,sans-serif;max-width:48rem;margin:3rem auto;\
             padding:0 1rem}}code{{word-break:break-all}}dd{{margin:0 0 .5rem}}\
             button{{font-size:1rem;padding:.4rem 1.2rem}}</style></head><body>\
             <h1>Open this decdn site?</h1>\
             <p>A page on another site sent you to a decdn site.</p>\
             <dl><dt>Site</dt><dd><code>{host}</code></dd>\
             <dt>Bundle</dt><dd><code>{hash}</code></dd>{namespace}</dl>\
             <p>Its pages pay for the content they load with the capabilities your \
             decdn gateway holds. Continue only if you trust this site. The gateway \
             remembers your choice until it stops.</p>\
             <form method=\"post\"><input type=\"hidden\" name=\"{FIELD}\" value=\"{token}\">\
             <button type=\"submit\">Continue</button></form></body></html>"
        );
        let mut response = hyper::Response::new(full(page));
        *response.status_mut() = StatusCode::OK;
        let headers = response.headers_mut();
        for (name, value) in [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CONTENT_SECURITY_POLICY, POLICY),
            (header::X_FRAME_OPTIONS, "DENY"),
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ] {
            headers.insert(name, header::HeaderValue::from_static(value));
        }
        response
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Whether `headers` are a browser's top-level navigation from a page on
/// another site, which a site answers with the interstitial until the user
/// continues.
pub(super) fn asks(headers: &HeaderMap) -> bool {
    header(headers, "sec-fetch-mode") == Some("navigate")
        && header(headers, "sec-fetch-dest") == Some("document")
        && matches!(
            header(headers, "sec-fetch-site"),
            Some("cross-site" | "same-site")
        )
}

/// Whether `headers` are the site's own post, from its own `origin`: the
/// only request that may carry the user's consent.
pub(super) fn from_own_page(headers: &HeaderMap, origin: &str) -> bool {
    header(headers, "sec-fetch-site") == Some("same-origin")
        && header(headers, "origin") == Some(origin)
}
