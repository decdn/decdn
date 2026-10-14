//! `decdn:` URIs (#2449): one string names a blob, a whole bundle, or one file
//! inside a bundle, with an optional namespace routing hint.
//!
//! ```text
//! decdn:<hash>[/<path>][?ns=<id>]
//! ```
//!
//! | URI | Names |
//! |---|---|
//! | `decdn:<hash>` | the raw blob |
//! | `decdn:<hash>/` | the whole bundle whose manifest has this hash |
//! | `decdn:<hash>/a/b.txt` | one entry of that bundle |
//! | any of the above with `?ns=42` | the same, with the namespace routing hint |
//!
//! - `<hash>` is 64 hex characters, bare: no `b3:` or `0x` prefix. Parsing
//!   accepts uppercase and [`DecdnUri`]'s `Display` writes lowercase.
//! - The URI has no `//` authority part, like `magnet:` and `urn:`.
//! - A trailing `/` marks a bundle. The URI author knows whether they published
//!   one, so the client never inspects content to decide.
//! - `<path>` is the manifest entry's relative POSIX path, each segment
//!   percent-encoded as an RFC 3986 `pchar`. It names exactly one manifest
//!   `path`, literally.
//! - `ns` is the namespace id in decimal (ADR 002), never `0`. It is a routing
//!   hint, not part of the identity, so it sits in the query.
//! - Any other query key is an error.
//!
//! The URI is client-side syntax only: it resolves to what the wire already
//! carries, a BLAKE3 hash plus the `namespace_id` hint.

use std::fmt;
use std::str::FromStr;

/// The scheme prefix every `decdn:` URI starts with.
pub const SCHEME: &str = "decdn:";

/// A parsed `decdn:` URI. `Display` writes its canonical form, which parses
/// back to an equal value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct DecdnUri {
    /// The BLAKE3 hash: of the blob, or of the bundle manifest.
    pub hash: [u8; 32],
    /// What the hash names.
    pub target: UriTarget,
    /// The namespace routing hint (ADR 002), never `Some(0)`.
    pub namespace: Option<u64>,
}

/// What a [`DecdnUri`]'s hash names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UriTarget {
    /// The raw blob (`decdn:<hash>`).
    Blob,
    /// The whole bundle whose manifest has the hash (`decdn:<hash>/`).
    Bundle,
    /// One entry of that bundle, by its decoded manifest path
    /// (`decdn:<hash>/a/b.txt`).
    BundleEntry(String),
}

/// Why a string is not a `decdn:` URI.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UriError {
    /// The string does not start with `decdn:`.
    MissingScheme,
    /// The URI has a `//` authority part, which the grammar does not allow.
    Authority,
    /// The hash is not 64 bare hex characters.
    Hash(String),
    /// The bundle entry path is malformed or unsafe.
    Path(String),
    /// The `ns` value is not a decimal id of at least 1.
    Namespace(String),
    /// The query carries something other than one `ns=<id>`.
    Query(String),
}

impl fmt::Display for UriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingScheme => write!(f, "not a decdn URI: expected `decdn:<hash>`"),
            Self::Authority => write!(
                f,
                "a decdn URI has no `//`: write `decdn:<hash>`, not `decdn://<hash>`"
            ),
            Self::Hash(why) => write!(
                f,
                "invalid hash in decdn URI ({why}): expected 64 hex characters with no \
                 `b3:` or `0x` prefix"
            ),
            Self::Path(why) => write!(f, "invalid bundle entry path in decdn URI: {why}"),
            Self::Namespace(why) => write!(
                f,
                "invalid namespace in decdn URI ({why}): expected `ns=<id>` with a decimal \
                 id of at least 1; omit `ns` for no namespace"
            ),
            Self::Query(why) => write!(
                f,
                "invalid query in decdn URI ({why}): the only key is `ns`"
            ),
        }
    }
}

impl std::error::Error for UriError {}

impl DecdnUri {
    /// A URI naming `target` under `hash`, with an optional namespace hint.
    #[must_use]
    pub const fn new(hash: [u8; 32], target: UriTarget, namespace: Option<u64>) -> Self {
        Self {
            hash,
            target,
            namespace,
        }
    }
}

impl FromStr for DecdnUri {
    type Err = UriError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rest = s.strip_prefix(SCHEME).ok_or(UriError::MissingScheme)?;
        if rest.starts_with("//") {
            return Err(UriError::Authority);
        }
        let (body, query) = match rest.split_once('?') {
            Some((body, query)) => (body, Some(query)),
            None => (rest, None),
        };
        let (hash, path) = match body.split_once('/') {
            Some((hash, path)) => (hash, Some(path)),
            None => (body, None),
        };
        let hash = parse_hash(hash)?;
        let target = match path {
            None => UriTarget::Blob,
            Some("") => UriTarget::Bundle,
            Some(path) => UriTarget::BundleEntry(decode_path(path)?),
        };
        let namespace = query.map(parse_query).transpose()?;
        Ok(Self {
            hash,
            target,
            namespace,
        })
    }
}

impl fmt::Display for DecdnUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(SCHEME)?;
        for b in self.hash {
            write!(f, "{b:02x}")?;
        }
        match &self.target {
            UriTarget::Blob => {}
            UriTarget::Bundle => f.write_str("/")?,
            UriTarget::BundleEntry(path) => {
                for segment in path.split('/') {
                    f.write_str("/")?;
                    for b in segment.bytes() {
                        if is_pchar(b) {
                            write!(f, "{}", char::from(b))?;
                        } else {
                            write!(f, "%{b:02X}")?;
                        }
                    }
                }
            }
        }
        if let Some(ns) = self.namespace {
            write!(f, "?ns={ns}")?;
        }
        Ok(())
    }
}

fn parse_hash(hex: &str) -> Result<[u8; 32], UriError> {
    if hex.len() != 64 {
        return Err(UriError::Hash(format!("{} characters", hex.len())));
    }
    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(UriError::Hash("not hex".to_owned()));
    }
    blake3::Hash::from_hex(hex.to_ascii_lowercase())
        .map(|h| *h.as_bytes())
        .map_err(|e| UriError::Hash(e.to_string()))
}

/// The decoded entry path: each `/`-separated segment percent-decoded, and
/// the whole held to the manifest's path rules (relative, no `.` or `..`, no
/// empty segment, UTF-8).
fn decode_path(raw: &str) -> Result<String, UriError> {
    let mut segments = Vec::new();
    for segment in raw.split('/') {
        let decoded = decode_segment(segment)?;
        if decoded.is_empty() {
            return Err(UriError::Path(
                "empty segment (a leading, trailing or doubled `/` inside the path)".to_owned(),
            ));
        }
        if decoded == "." || decoded == ".." {
            return Err(UriError::Path(format!("`{decoded}` segment")));
        }
        if decoded.contains('/') {
            return Err(UriError::Path(
                "encoded `/` (`%2F`) in a segment".to_owned(),
            ));
        }
        if decoded.contains('\0') {
            return Err(UriError::Path("NUL byte".to_owned()));
        }
        segments.push(decoded);
    }
    Ok(segments.join("/"))
}

fn decode_segment(segment: &str) -> Result<String, UriError> {
    let mut out = Vec::with_capacity(segment.len());
    let mut bytes = segment.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hi = bytes.next().and_then(hex_value);
            let lo = bytes.next().and_then(hex_value);
            match (hi, lo) {
                (Some(hi), Some(lo)) => out.push(hi << 4 | lo),
                _ => return Err(UriError::Path("bad `%` escape".to_owned())),
            }
        } else if is_pchar(b) {
            out.push(b);
        } else {
            return Err(UriError::Path(format!(
                "character {:?} must be percent-encoded",
                char::from(b)
            )));
        }
    }
    String::from_utf8(out).map_err(|_| UriError::Path("not UTF-8 once decoded".to_owned()))
}

fn hex_value(b: u8) -> Option<u8> {
    char::from(b)
        .to_digit(16)
        .and_then(|d| u8::try_from(d).ok())
}

/// An RFC 3986 `pchar` other than `%`: unreserved, sub-delims, `:` and `@`.
const fn is_pchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
        )
}

fn parse_query(query: &str) -> Result<u64, UriError> {
    let mut namespace = None;
    for pair in query.split('&') {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| UriError::Query(format!("`{pair}` is not `key=value`")))?;
        if key != "ns" {
            return Err(UriError::Query(format!("unknown key `{key}`")));
        }
        if namespace.is_some() {
            return Err(UriError::Query("`ns` given twice".to_owned()));
        }
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(UriError::Namespace(format!(
                "`{value}` is not a decimal id"
            )));
        }
        let id: u64 = value
            .parse()
            .map_err(|_| UriError::Namespace(format!("`{value}` is out of range")))?;
        if id == 0 {
            return Err(UriError::Namespace("0 means no namespace".to_owned()));
        }
        namespace = Some(id);
    }
    namespace.ok_or_else(|| UriError::Query("empty query".to_owned()))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
