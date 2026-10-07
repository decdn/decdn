use super::*;

#[test]
fn parse_origin_url_appends_trailing_slash() -> anyhow::Result<()> {
    let url = parse_origin_url("https://origin.example/v1")?;
    anyhow::ensure!(
        url.as_url().as_str() == "https://origin.example/v1/",
        "got: {url}"
    );
    // The full path segment must survive `join` — not collapse to `/`.
    let joined = url.as_url().join("abcdef")?;
    anyhow::ensure!(
        joined.as_str() == "https://origin.example/v1/abcdef",
        "join produced: {joined}"
    );
    Ok(())
}

#[test]
fn parse_origin_url_preserves_existing_trailing_slash() -> anyhow::Result<()> {
    let url = parse_origin_url("https://origin.example/")?;
    anyhow::ensure!(
        url.as_url().as_str() == "https://origin.example/",
        "got: {url}"
    );
    Ok(())
}

#[test]
fn parse_origin_url_rejects_query_string() -> anyhow::Result<()> {
    // `.join(hex)` on a query-bearing base drops the query silently —
    // operators would never notice. Reject at parse time instead.
    let err = parse_origin_url("https://origin.example/v1?token=abc")
        .err()
        .ok_or_else(|| anyhow::anyhow!("query-bearing URL should have been rejected"))?
        .to_string();
    anyhow::ensure!(
        err.contains("must not contain a query string"),
        "error lacked context: {err}"
    );
    Ok(())
}

#[test]
fn parse_origin_url_rejects_fragment() -> anyhow::Result<()> {
    let err = parse_origin_url("https://origin.example/v1#frag")
        .err()
        .ok_or_else(|| anyhow::anyhow!("fragment URL should have been rejected"))?
        .to_string();
    anyhow::ensure!(
        err.contains("must not contain a fragment"),
        "error lacked context: {err}"
    );
    Ok(())
}

#[test]
fn parse_origin_url_accepts_http_and_https() -> anyhow::Result<()> {
    parse_origin_url("http://origin.example")?;
    parse_origin_url("https://origin.example")?;
    Ok(())
}

#[test]
fn parse_origin_url_rejects_non_http_schemes() -> anyhow::Result<()> {
    for raw in [
        "file:///etc/passwd",
        "ftp://origin.example",
        "ws://origin.example",
    ] {
        let err = parse_origin_url(raw)
            .err()
            .ok_or_else(|| anyhow::anyhow!("{raw} should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("unsupported origin URL scheme"),
            "error for {raw} lacked scheme context: {err}"
        );
    }
    Ok(())
}

#[test]
fn redact_for_log_strips_user_and_password() -> anyhow::Result<()> {
    let url = Url::parse("https://user:secret@host.example/path")?;
    let redacted = redact_for_log(&url);
    anyhow::ensure!(
        !redacted.contains("secret") && !redacted.contains("user"),
        "redaction leaked credentials: {redacted}"
    );
    anyhow::ensure!(
        redacted.contains("host.example"),
        "redaction dropped host: {redacted}"
    );
    Ok(())
}

#[test]
fn redact_for_log_strips_username_only() -> anyhow::Result<()> {
    let url = Url::parse("https://someuser@host.example/")?;
    let redacted = redact_for_log(&url);
    anyhow::ensure!(
        !redacted.contains("someuser"),
        "redaction kept username: {redacted}"
    );
    Ok(())
}

#[test]
fn redact_raw_for_log_emits_placeholder_for_unparseable() -> anyhow::Result<()> {
    anyhow::ensure!(redact_raw_for_log("not a url") == "<unparseable URL>");
    anyhow::ensure!(redact_raw_for_log("   ") == "<unparseable URL>");
    Ok(())
}

#[test]
fn parse_origin_url_errors_do_not_leak_credentials() -> anyhow::Result<()> {
    // URL parses cleanly but has a rejected query string. Credentials
    // must not appear in the rejection message — operators routinely
    // share config errors from logs.
    let err = parse_origin_url("https://user:secret@host.example/path?token=abc")
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected rejection"))?
        .to_string();
    anyhow::ensure!(!err.contains("secret"), "error leaked password: {err}");
    anyhow::ensure!(!err.contains("user"), "error leaked username: {err}");
    Ok(())
}

#[test]
fn parse_origin_url_rejects_unparseable_input() -> anyhow::Result<()> {
    let err = parse_origin_url("not a url")
        .err()
        .ok_or_else(|| anyhow::anyhow!("bare text should have been rejected"))?
        .to_string();
    anyhow::ensure!(
        err.contains("invalid origin base URL"),
        "error lacked context: {err}"
    );
    Ok(())
}
