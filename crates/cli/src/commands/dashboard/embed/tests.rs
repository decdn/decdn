use super::*;

fn header<'a>(response: &'a Response<Body>, name: &header::HeaderName) -> Option<&'a str> {
    response.headers().get(name).and_then(|v| v.to_str().ok())
}

/// The page is the Vite build when one was embedded, and otherwise the
/// fallback that says how to build it. Either way it carries the policy.
#[test]
fn the_page_is_the_build_or_the_fallback() {
    let fallback = INDEX_HTML.contains("decdn-dashboard-fallback");
    assert_eq!(built(), !fallback, "built() agrees with the embedded page");
    if built() {
        assert!(INDEX_HTML.contains("/_dashboard/app.js"), "{INDEX_HTML}");
        assert!(!APP_JS.is_empty());
    } else {
        assert!(INDEX_HTML.contains("pnpm"), "{INDEX_HTML}");
        assert!(
            INDEX_HTML.contains("cargo build -p decdn-cli"),
            "{INDEX_HTML}"
        );
    }
    let page = page();
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(
        header(&page, &header::CONTENT_TYPE),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(header(&page, &header::CONTENT_SECURITY_POLICY), Some(CSP));
    assert_eq!(header(&page, &header::CACHE_CONTROL), Some("no-cache"));
    assert_eq!(
        header(&page, &header::X_CONTENT_TYPE_OPTIONS),
        Some("nosniff")
    );
    assert_eq!(header(&page, &header::REFERRER_POLICY), Some("no-referrer"));
}

/// Each asset answers with its own type; any other path is none of them.
#[test]
fn assets_carry_their_types() {
    let js =
        asset("/_dashboard/app.js").map(|r| header(&r, &header::CONTENT_TYPE).map(str::to_owned));
    assert_eq!(js, Some(Some("text/javascript; charset=utf-8".to_owned())));
    let css =
        asset("/_dashboard/app.css").map(|r| header(&r, &header::CONTENT_TYPE).map(str::to_owned));
    assert_eq!(css, Some(Some("text/css; charset=utf-8".to_owned())));
    for other in [
        "/_dashboard/",
        "/_dashboard/index.html",
        "/app.js",
        "/_dashboard/app.js.map",
    ] {
        assert!(asset(other).is_none(), "{other}");
    }
    let js = asset("/_dashboard/app.js");
    assert_eq!(
        js.as_ref()
            .and_then(|r| header(r, &header::X_CONTENT_TYPE_OPTIONS)),
        Some("nosniff")
    );
}
