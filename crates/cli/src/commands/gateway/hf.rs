//! The `HF_ENDPOINT` shim on a namespace host: the resolve paths
//! `huggingface_hub` fetches files by, mapped onto bundle entries.
//!
//! With `HF_ENDPOINT=https://<ns>.<host>`, the hub client asks for
//! `/<repo>/resolve/<revision>/<file>` (`<repo>` is `org/name`, or
//! `datasets/`, `spaces/` or `models/` before it), `HEAD` first for the
//! metadata, then `GET` with a `Range` to resume. On a namespace host that
//! path is the entry `<repo>/<file>` of the bundle the namespace points at,
//! under the revision `main` or the bundle's commit, the first forty hex
//! characters of its manifest hash. The answer carries the headers the
//! client reads: the commit, the entry hash as the entity tag and the linked
//! entity tag, and the entry's size as the linked size; the body is the
//! entry byte for byte, never rewritten, since the client stores it under
//! the entry hash. A `HEAD` under `main` redirects to the commit path, which
//! the client then downloads from: a pointer that moves between the two
//! requests makes the download fail instead of caching another bundle's
//! bytes under the first bundle's commit. A 404 names its cause in
//! `X-Error-Code`, which is how the client tells a missing file or revision
//! from a lost connection. Any `/<repo>/resolve/<rev>/<file>` path on a
//! namespace host is this shim's, so an entry at such a literal path is not
//! reachable there.

use hyper::HeaderMap;
use hyper::header::{self, HeaderName, HeaderValue};

/// What a path on a namespace host is to the shim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HfPath {
    /// A resolve path under a known revision: the entry it names, and
    /// whether the revision was `main` rather than the commit.
    Entry {
        /// The entry path, `<repo>/<file>`.
        entry: String,
        /// The revision was `main`: a `HEAD` redirects to [`pinned`].
        main: bool,
    },
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

/// The repo types the hub puts before `org/name` in a path.
const REPO_TYPES: &[&str] = &["datasets", "spaces", "models"];

/// What `path` (with its leading `/`) is to the shim on the bundle `root`.
/// A resolve path is a repo id (`org/name`, with a repo type before it or
/// not), `resolve`, a revision, and at least one file segment, none of them
/// empty. The delimiter is looked for after the repo id, so a repo named
/// `resolve` is a repo.
pub(super) fn resolve(path: &str, root: &[u8; 32]) -> HfPath {
    let Some(rest) = path.strip_prefix('/') else {
        return HfPath::NotResolve;
    };
    let segments: Vec<&str> = rest.split('/').collect();
    let typed = segments.first().is_some_and(|s| REPO_TYPES.contains(s));
    let at = if typed { 3 } else { 2 };
    if segments.get(at) != Some(&"resolve") {
        return HfPath::NotResolve;
    }
    let (repo, after) = segments.split_at(at);
    let Some((revision, file)) = after.get(1..).and_then(|after| after.split_first()) else {
        return HfPath::NotResolve;
    };
    if revision.is_empty() || file.is_empty() || repo.iter().chain(file).any(|s| s.is_empty()) {
        return HfPath::NotResolve;
    }
    if *revision != "main" && *revision != commit(root) {
        return HfPath::UnknownRevision;
    }
    HfPath::Entry {
        entry: format!("{}/{}", repo.join("/"), file.join("/")),
        main: *revision == "main",
    }
}

/// The resolve path of `entry` (`<repo>/<file>`) under the commit of the
/// bundle `root`: where a `HEAD` under `main` sends the client.
pub(super) fn pinned(entry: &str, root: &[u8; 32]) -> String {
    let typed = entry
        .split('/')
        .next()
        .is_some_and(|first| REPO_TYPES.contains(&first));
    let repo_segments = if typed { 3 } else { 2 };
    // The `/` after the repo id; an entry always has one.
    let Some((at, _)) = entry.match_indices('/').nth(repo_segments - 1) else {
        return format!("/{entry}");
    };
    let (repo, file) = entry.split_at(at);
    format!("/{repo}/resolve/{}{file}", commit(root))
}

/// The `X-Error-Code` the hub client reads on a 404: the revision is not
/// the bundle's.
pub(super) const REVISION_NOT_FOUND: &str = "RevisionNotFound";

/// The `X-Error-Code` the hub client reads on a 404: the bundle has no such
/// entry. The client also keeps a negative-cache marker under the commit.
pub(super) const ENTRY_NOT_FOUND: &str = "EntryNotFound";

/// Mark a 404 for the hub client: `X-Error-Code: code`, and the commit of
/// the bundle `root` when the revision was known.
pub(super) fn not_found(headers: &mut HeaderMap, code: &'static str, root: Option<&[u8; 32]>) {
    headers.insert(
        HeaderName::from_static("x-error-code"),
        HeaderValue::from_static(code),
    );
    if let Some(value) = root.and_then(|root| HeaderValue::from_str(&commit(root)).ok()) {
        headers.insert(HeaderName::from_static("x-repo-commit"), value);
    }
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
