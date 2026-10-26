//! The gateway's dashboard: a Vite app (`crates/cli/dashboard/`) that polls
//! `GET /_status`, embedded by `build.rs` as three fixed files. A build with
//! no Vite output embeds a page that says how to make one ([`built`] tells
//! the two apart).
//!
//! The page carries a policy that loads script and style from the gateway
//! alone, and no inline script or style, so a page served beside site
//! content can run nothing but the dashboard's own code.

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{Response, StatusCode, header};

use super::{Body, text};

const INDEX_HTML: &str = include_str!(concat!(env!("OUT_DIR"), "/dashboard/index.html"));
const APP_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/dashboard/app.js"));
const APP_CSS: &str = include_str!(concat!(env!("OUT_DIR"), "/dashboard/app.css"));

/// The dashboard's content security policy: its own script, style and API
/// only, in no frame.
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; \
                   img-src 'self' data:; base-uri 'none'; form-action 'none'; \
                   frame-ancestors 'none'";

/// Whether this binary embeds the Vite build, not the fallback page.
pub(super) fn built() -> bool {
    env!("DECDN_DASHBOARD_BUILT") == "1"
}

/// The dashboard page.
pub(super) fn page() -> Response<Body> {
    respond(INDEX_HTML, "text/html; charset=utf-8")
}

/// The asset at `path`, or `None` when `path` names none.
pub(super) fn asset(path: &str) -> Option<Response<Body>> {
    match path {
        "/_dashboard/app.js" => Some(respond(APP_JS, "text/javascript; charset=utf-8")),
        "/_dashboard/app.css" => Some(respond(APP_CSS, "text/css; charset=utf-8")),
        _ => None,
    }
}

fn respond(body: &'static str, content_type: &'static str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_SECURITY_POLICY, CSP)
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::REFERRER_POLICY, "no-referrer")
        .body(
            Full::new(Bytes::from_static(body.as_bytes()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap_or_else(|_| {
            text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "bad dashboard response\n",
            )
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
