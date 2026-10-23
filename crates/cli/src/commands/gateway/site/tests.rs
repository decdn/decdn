use super::*;

fn site(namespace: Option<u64>) -> Site {
    Site {
        hash: *blake3::hash(b"site").as_bytes(),
        namespace,
    }
}

#[test]
fn a_site_host_round_trips() {
    for namespace in [None, Some(7), Some(u64::MAX)] {
        let site = site(namespace);
        let host = site.host();
        assert_eq!(Site::from_host(&format!("{host}:8484"), 8484), Some(site));
    }
    let label = site(None).host();
    let label = label.strip_suffix(".localhost").unwrap();
    assert_eq!(label.len(), LABEL_LEN);
    assert!(
        label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    );
}

#[test]
fn a_malformed_site_host_names_no_site() {
    let host = site(None).host();
    let label = host.strip_suffix(".localhost").unwrap();
    for bad in [
        // The wrong port, or none.
        format!("{host}:9999"),
        format!("{host}:18484"),
        host.clone(),
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
        format!("0.{host}:8484"),
        format!("07.{host}:8484"),
        format!("x.{host}:8484"),
        format!(".{host}:8484"),
        format!("1.2.{host}:8484"),
        "localhost:8484".to_owned(),
        "127.0.0.1:8484".to_owned(),
    ] {
        assert_eq!(Site::from_host(&bad, 8484), None, "{bad}");
    }
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
    let csp = content_security_policy("127.0.0.1:8484".parse().unwrap());
    assert!(csp.starts_with("default-src 'self' "), "{csp}");
    assert!(
        csp.contains("connect-src 'self' http://127.0.0.1:8484"),
        "{csp}"
    );
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
