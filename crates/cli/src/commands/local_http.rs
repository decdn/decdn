//! What the `decdn` servers that answer on a loopback address share: the
//! token their operator routes ask for, the `Host` names they answer, and the
//! plain responses they build.

use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context as _;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::header::HeaderMap;
use hyper::{Response, StatusCode, header};

/// The request header carrying a loopback server's token.
pub(crate) const TOKEN_HEADER: &str = "x-decdn-token";

/// The body of every response a loopback server builds.
pub(crate) type Body = BoxBody<Bytes, std::io::Error>;

/// The `Host` header values a server on `listen` answers: its own loopback
/// address under each name a browser may use. Anything else is a
/// DNS-rebinding attempt.
pub(crate) fn allowed_hosts(listen: SocketAddr) -> Vec<String> {
    let port = listen.port();
    vec![
        listen.to_string(),
        format!("localhost:{port}"),
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
    ]
}

/// The request's `Host` header, or `""` when it has none the server can
/// read.
pub(crate) fn host_header(headers: &HeaderMap) -> &str {
    headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
}

/// Create `dir` owner-only.
pub(crate) fn private_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("restrict {}", dir.display()))?;
    }
    Ok(())
}

/// The token in `path`, or a fresh random one written there owner-only.
pub(crate) fn load_or_create_token(path: &Path) -> anyhow::Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim().to_owned();
        anyhow::ensure!(
            token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()),
            "{} is not a decdn token; delete it to make a new one",
            path.display()
        );
        return Ok(token);
    }
    let token = alloy::hex::encode(rand::random::<[u8; 32]>());
    std::fs::write(path, &token).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restrict {}", path.display()))?;
    }
    Ok(token)
}

/// Compare `given` with `token` without leaking where they differ.
pub(crate) fn token_matches(token: &str, given: &str) -> bool {
    token.len() == given.len()
        && token
            .bytes()
            .zip(given.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

/// Whether `headers` carry `token` in [`TOKEN_HEADER`].
pub(crate) fn carries_token(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(TOKEN_HEADER)
        .and_then(|t| t.to_str().ok())
        .is_some_and(|given| token_matches(token, given))
}

pub(crate) fn full(body: String) -> Body {
    Full::new(Bytes::from(body))
        .map_err(|never| match never {})
        .boxed()
}

pub(crate) fn text(status: StatusCode, body: &str) -> Response<Body> {
    typed(status, body.to_owned(), "text/plain; charset=utf-8")
}

pub(crate) fn json<T: serde::Serialize + ?Sized>(status: StatusCode, value: &T) -> Response<Body> {
    let body = serde_json::to_string(value).unwrap_or_else(|_| "null".into());
    typed(status, body, "application/json")
}

pub(crate) fn typed(
    status: StatusCode,
    body: String,
    content_type: &'static str,
) -> Response<Body> {
    let mut response = Response::new(full(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    response
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
