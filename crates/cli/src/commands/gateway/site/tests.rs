use super::*;

fn site(namespace: Option<u64>) -> Site {
    Site {
        hash: *blake3::hash(b"site").as_bytes(),
        namespace,
    }
}

fn loopback() -> SiteDomain {
    SiteDomain::Loopback {
        listen: "127.0.0.1:8484".parse().unwrap(),
    }
}

fn public(origin: &str) -> SiteDomain {
    SiteDomain::Public {
        origin: origin.parse().unwrap(),
    }
}

/// The hosts every domain gives a site, with each namespace form.
fn domains() -> Vec<(SiteDomain, &'static str)> {
    vec![
        (loopback(), "localhost:8484"),
        (public("https://decdn.test"), "decdn.test"),
        (public("http://localhost:8080"), "localhost:8080"),
    ]
}

#[test]
fn a_site_host_round_trips() {
    for (domain, suffix) in domains() {
        for namespace in [None, Some(7), Some(u64::MAX)] {
            let site = site(namespace);
            let host = domain.site_host(&site);
            assert!(host.ends_with(&format!(".{suffix}")), "{host}");
            assert_eq!(domain.route(&host), Some(Route::Site(site)), "{host}");
        }
    }
    let host = loopback().site_host(&site(None));
    let label = host.strip_suffix(".localhost:8484").unwrap();
    assert_eq!(label.len(), LABEL_LEN);
    assert!(
        label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    );
}

/// A namespace's own host on a public domain, and the route back to it.
#[test]
fn a_namespace_host_round_trips() {
    let domain = public("https://decdn.test");
    assert_eq!(domain.namespace_host(7), "7.decdn.test");
    assert_eq!(domain.route("7.decdn.test"), Some(Route::Namespace(7)));
    let ported = public("http://localhost:8080");
    assert_eq!(ported.namespace_host(7), "7.localhost:8080");
    assert_eq!(ported.route("7.localhost:8080"), Some(Route::Namespace(7)));
}

#[test]
fn a_malformed_site_host_names_no_site() {
    let domain = loopback();
    let host = domain.site_host(&site(None));
    let name = host.strip_suffix(":8484").unwrap();
    let label = name.strip_suffix(".localhost").unwrap();
    for bad in [
        // The wrong port, or none.
        format!("{name}:9999"),
        format!("{name}:18484"),
        name.to_owned(),
        // Not under `localhost`.
        format!("{label}.example.com:8484"),
        format!("{label}localhost:8484"),
        // Uppercase, short, long, and outside the alphabet.
        format!("{}.localhost:8484", label.to_ascii_uppercase()),
        format!("{}.localhost:8484", &label[1..]),
        format!("{label}a.localhost:8484"),
        format!("{}1.localhost:8484", &label[1..]),
        // Nonzero trailing bits: a second spelling of a hash.
        format!("{}b.localhost:8484", &label[..LABEL_LEN - 1]),
        // A namespace of 0, with a leading zero, or not decimal.
        format!("0.{host}"),
        format!("07.{host}"),
        format!("x.{host}"),
        format!(".{host}"),
        format!("1.2.{host}"),
        // On loopback a namespace alone is no site: the gateway's own names
        // are the caller's to allow.
        "7.localhost:8484".to_owned(),
        "localhost:8484".to_owned(),
        "127.0.0.1:8484".to_owned(),
    ] {
        assert_eq!(domain.route(&bad), None, "{bad}");
    }
}

#[test]
fn a_public_domain_routes_its_bare_host_sites_and_namespaces() {
    let domain = public("https://decdn.test");
    let site = site(Some(7));
    let hash = domain.site_host(&self::site(None));
    let label = hash.strip_suffix(".decdn.test").unwrap();
    assert_eq!(domain.route("decdn.test"), Some(Route::Bare));
    assert_eq!(
        domain.route(&format!("7.{label}.decdn.test")),
        Some(Route::Site(site))
    );
    assert_eq!(domain.route("7.decdn.test"), Some(Route::Namespace(7)));
    assert_eq!(
        domain.route("18446744073709551615.decdn.test"),
        Some(Route::Namespace(u64::MAX))
    );
    for bad in [
        "decdn.test:443",
        "evil.com",
        "decdn.test.evil.com",
        "0.decdn.test",
        "07.decdn.test",
        "1.2.decdn.test",
        "x.decdn.test",
        "18446744073709551616.decdn.test",
        "localhost:8484",
        "127.0.0.1:8484",
        "",
    ] {
        assert_eq!(domain.route(bad), None, "{bad}");
    }
    assert_eq!(
        domain.route(&format!("{}.decdn.test", label.to_ascii_uppercase())),
        None
    );
    assert_eq!(domain.route(&format!("{label}.decdn.test.evil.com")), None);
}

#[test]
fn a_public_origin_with_a_port_keeps_it_in_every_host() {
    let domain = public("http://localhost:8080");
    let host = domain.site_host(&site(None));
    assert!(host.ends_with(".localhost:8080"), "{host}");
    assert_eq!(domain.route("localhost:8080"), Some(Route::Bare));
    assert_eq!(domain.route("localhost"), None);
    assert_eq!(domain.route("localhost:8484"), None);
    assert_eq!(domain.route(&host), Some(Route::Site(site(None))));
}

#[test]
fn an_origin_carries_the_scheme_and_the_port_of_its_domain() {
    let hash = site(None);
    let host = loopback().site_host(&hash);
    assert_eq!(loopback().origin(&host), format!("http://{host}"));
    assert!(host.ends_with(":8484"));
    let domain = public("https://decdn.test");
    let host = domain.site_host(&hash);
    assert_eq!(domain.origin(&host), format!("https://{host}"));
    assert_eq!(domain.origin("decdn.test"), "https://decdn.test");
    let domain = public("http://localhost:8080");
    assert_eq!(domain.origin("localhost:8080"), "http://localhost:8080");
}

fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(*name, value.parse().unwrap());
    }
    map
}

#[test]
fn a_site_spends_for_its_own_pages_the_user_and_top_level_navigation() {
    for allowed in [
        headers(&[
            ("sec-fetch-site", "same-origin"),
            ("sec-fetch-mode", "no-cors"),
        ]),
        headers(&[("sec-fetch-site", "none"), ("sec-fetch-mode", "navigate")]),
        headers(&[
            ("sec-fetch-site", "cross-site"),
            ("sec-fetch-mode", "navigate"),
            ("sec-fetch-dest", "document"),
        ]),
        headers(&[
            ("sec-fetch-site", "same-site"),
            ("sec-fetch-mode", "navigate"),
            ("sec-fetch-dest", "document"),
        ]),
    ] {
        assert!(may_spend(&allowed, false).is_ok(), "{allowed:?}");
    }
}

#[test]
fn other_pages_cannot_make_a_site_spend() {
    for refused in [
        headers(&[
            ("sec-fetch-site", "cross-site"),
            ("sec-fetch-mode", "no-cors"),
            ("sec-fetch-dest", "image"),
        ]),
        headers(&[
            ("sec-fetch-site", "cross-site"),
            ("sec-fetch-mode", "navigate"),
            ("sec-fetch-dest", "iframe"),
        ]),
        headers(&[
            ("sec-fetch-site", "same-site"),
            ("sec-fetch-mode", "cors"),
            ("sec-fetch-dest", "empty"),
        ]),
    ] {
        assert_eq!(
            may_spend(&refused, false).unwrap_err().0,
            StatusCode::FORBIDDEN,
            "{refused:?}"
        );
    }
}

#[test]
fn outside_a_browser_a_site_needs_the_token() {
    assert_eq!(
        may_spend(&HeaderMap::new(), false).unwrap_err().0,
        StatusCode::UNAUTHORIZED
    );
    assert!(may_spend(&HeaderMap::new(), true).is_ok());
}

#[test]
fn the_policy_confines_a_page_to_its_origin_and_the_gateway_api() {
    let csp = content_security_policy(&loopback());
    assert!(csp.starts_with("default-src 'self' "), "{csp}");
    assert!(
        csp.contains("connect-src 'self' http://127.0.0.1:8484"),
        "{csp}"
    );
}

/// A public gateway has no API a page may call, so its pages reach only
/// their own origin.
#[test]
fn the_public_policy_names_no_other_origin() {
    let csp = content_security_policy(&public("https://decdn.test"));
    assert!(csp.starts_with("default-src 'self' "), "{csp}");
    assert!(csp.ends_with("connect-src 'self'"), "{csp}");
    assert!(!csp.contains("http"), "{csp}");
}

#[test]
fn a_site_serves_markup_and_scripts_as_themselves() {
    assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
    assert_eq!(content_type("site.css"), "text/css; charset=utf-8");
    assert_eq!(content_type("app.js"), "text/javascript; charset=utf-8");
    assert_eq!(content_type("logo.svg"), "image/svg+xml");
    assert_eq!(content_type("photo.png"), "image/png");
    assert_eq!(content_type("no-extension"), "application/octet-stream");
}
