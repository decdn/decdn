//! The `HF_ENDPOINT` shim on a namespace host: the resolve paths
//! `huggingface_hub` fetches files by, mapped onto bundle entries.
//!
//! With `HF_ENDPOINT=https://<ns>.<host>`, the hub client asks for
//! `/<repo>/resolve/<revision>/<file>` (`<repo>` is `org/name`, or
//! `datasets/org/name`), `HEAD` first for the metadata, then `GET` with a
//! `Range` to resume. On a namespace host that path is the entry
//! `<repo>/<file>` of the bundle the namespace points at, under the revision
//! `main` or the bundle's commit, the first forty hex characters of its
//! manifest hash. The answer carries the headers the client reads: the
//! commit, the entry hash as the entity tag and the linked entity tag, and
//! the entry's size as the linked size. There is no redirect. Any
//! `/<a>/<b>[/…]/resolve/<rev>/<file>` path on a namespace host is this
//! shim's, so an entry at such a literal path is not reachable there.

use hyper::HeaderMap;
use hyper::header::{self, HeaderName, HeaderValue};

/// What a path on a namespace host is to the shim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HfPath {
    /// A resolve path under a known revision: the entry it names.
    Entry(String),
    /// A resolve path under a revision the bundle is not.
    UnknownRevision,
    /// Not a resolve path: an entry path as on any site.
    NotResolve,
}

/// The bundle's commit as the hub names one: the first forty hex characters
/// of its manifest hash.
pub(super) fn commit(root: &[u8; 32]) -> String {
    let mut hex = alloy::hex::encode(root);
    hex.truncate(40);
    hex
}

/// What `path` (with its leading `/`) is to the shim on the bundle `root`.
/// A resolve path has at least two segments before `resolve`, a revision,
/// and at least one file segment, none of them empty.
pub(super) fn resolve(path: &str, root: &[u8; 32]) -> HfPath {
    let Some(rest) = path.strip_prefix('/') else {
        return HfPath::NotResolve;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    let Some(at) = segments.iter().position(|s| *s == "resolve") else {
        return HfPath::NotResolve;
    };
    let (repo, after) = segments.split_at(at);
    let Some((revision, file)) = after.get(1..).and_then(|after| after.split_first()) else {
        return HfPath::NotResolve;
    };
    if repo.len() < 2
        || revision.is_empty()
        || file.is_empty()
        || repo.iter().chain(file).any(|s| s.is_empty())
    {
        return HfPath::NotResolve;
    }
    if *revision != "main" && *revision != commit(root) {
        return HfPath::UnknownRevision;
    }
    HfPath::Entry(format!("{}/{}", repo.join("/"), file.join("/")))
}

/// Add the hub's headers to an answer for an entry of the bundle `root`:
/// `X-Repo-Commit`; `X-Linked-Etag` as the entity tag already set; and
/// `X-Linked-Size` as the whole entry's size, from `Content-Range` on a
/// partial answer, else `Content-Length`.
pub(super) fn decorate(headers: &mut HeaderMap, root: &[u8; 32]) {
    if let Ok(value) = HeaderValue::from_str(&commit(root)) {
        headers.insert(HeaderName::from_static("x-repo-commit"), value);
    }
    if let Some(etag) = headers.get(header::ETAG).cloned() {
        headers.insert(HeaderName::from_static("x-linked-etag"), etag);
    }
    let total = headers
        .get(header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit_once('/'))
        .map(|(_, total)| total.to_owned())
        .or_else(|| {
            headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        });
    if let Some(value) = total.and_then(|t| HeaderValue::from_str(&t).ok()) {
        headers.insert(HeaderName::from_static("x-linked-size"), value);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests;
