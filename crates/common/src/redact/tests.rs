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
    let two = strip_urls("boom for url (https://a.example/K1) then for url (https://b.example/K2)");
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
    let inner = anyhow::anyhow!("error sending request for url (https://eth.example/v3/SECRETKEY)");
    let chained = inner.context("failed to read minCapacityMbps (RPC reachable?)");
    let out = sanitize_err_chain(&chained);
    assert!(out.contains("failed to read minCapacityMbps"));
    assert!(!out.contains("SECRETKEY"));
    assert!(!out.contains("eth.example"));
}

/// A test error layer with a fixed Display and an optional source.
#[derive(Debug)]
struct Layer {
    msg: String,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Layer {
    fn new(msg: &str, source: Option<Layer>) -> Self {
        Self {
            msg: msg.to_owned(),
            source: source.map(|s| Box::new(s) as Box<dyn std::error::Error + Send + Sync>),
        }
    }
}

impl std::fmt::Display for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Layer {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|s| s as &(dyn std::error::Error + 'static))
    }
}

#[test]
fn sanitize_error_sources_keeps_the_class_beneath_a_url_layer() {
    // The reqwest shape: the top layer names the URL, the class is a source.
    let err = Layer::new(
        "error sending request for url (https://eth.example/v3/SECRETKEY)",
        Some(Layer::new(
            "client error (Connect)",
            Some(Layer::new("operation timed out", None)),
        )),
    );
    let out = sanitize_error_sources(&err);
    assert_eq!(
        out,
        "error sending request: client error (Connect): operation timed out"
    );
    assert!(!out.contains("SECRETKEY") && !out.contains("eth.example"));
    // The top-level Display alone loses the class.
    assert!(!sanitize_rpc_display(&err).contains("timed out"));
}

#[test]
fn sanitize_error_sources_skips_repeated_layers() {
    // A `"transport: {0}"` wrapper ends its message with its source, and a
    // `"{0}"` wrapper above it repeats that text verbatim.
    let inner = Layer::new("connection refused (os error 61)", None);
    let quoted = Layer::new("transport: connection refused (os error 61)", Some(inner));
    let transparent = Layer::new("transport: connection refused (os error 61)", Some(quoted));
    let out = sanitize_error_sources(&transparent);
    assert_eq!(out, "transport: connection refused (os error 61)");
}

#[test]
fn sanitize_error_sources_keeps_a_cause_that_only_appears_inside_the_outer_message() {
    // "timeout" appears in the outer message but does not end it, so the
    // source is a distinct cause, not a repeat.
    let err = Layer::new(
        "request timeout exceeded",
        Some(Layer::new("timeout", None)),
    );
    assert_eq!(
        sanitize_error_sources(&err),
        "request timeout exceeded: timeout"
    );
}

#[test]
fn sanitize_error_sources_redacts_a_url_in_a_deeper_layer() {
    let err = Layer::new(
        "getPool call failed",
        Some(Layer::new(
            "reach https://eth.example/rpc?apikey=SECRETKEY failed",
            None,
        )),
    );
    let out = sanitize_error_sources(&err);
    assert_eq!(out, "getPool call failed: reach <redacted-url> failed");
}

#[test]
fn sanitize_err_chain_dedupes_and_strips_each_layer() {
    // alloy's shape under `chain_events::timed`: a `"{0}"` transport layer
    // repeats the reqwest text, and the URL token would swallow a joined
    // `": "` separator if the chain were stripped as one string.
    let reqwest = Layer::new(
        "error sending request for url (https://eth.example/v3/SECRETKEY)",
        Some(Layer::new("client error (Connect)", None)),
    );
    let transport = Layer::new(
        "error sending request for url (https://eth.example/v3/SECRETKEY)",
        Some(reqwest),
    );
    let err = anyhow::Error::new(transport).context("admit getPool");
    assert_eq!(
        sanitize_err_chain(&err),
        "admit getPool: error sending request: client error (Connect)"
    );
}

#[test]
fn sanitize_rpc_display_strips_url_from_plain_error() {
    let err = std::io::Error::other("request for url (https://eth.example/v3/SECRETKEY) timed out");
    let out = sanitize_rpc_display(&err);
    assert!(!out.contains("SECRETKEY"));
    assert!(out.contains("timed out"));
}
