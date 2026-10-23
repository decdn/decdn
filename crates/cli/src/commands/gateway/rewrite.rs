//! `decdn:` URIs in a site's HTML and CSS, rewritten as they stream (#2457).
//!
//! A site names content outside its own bundle with `decdn:` URIs. On a site
//! origin each one becomes a root-relative path on that origin,
//! `decdn:<hash>…` to `/_decdn/<hash>…`, so a served page names no gateway
//! host or port. A URI here is the scheme followed by 64 hex characters, at
//! the start of a value or after a character that cannot end a scheme name.
//!
//! HTML streams through `lol_html`, which rewrites URL attributes, `style`
//! attributes and `<style>` elements. CSS is scanned as bytes. URLs a script
//! builds at runtime stay as they are.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use lol_html::html_content::ContentType;
use lol_html::send::{HtmlRewriter, Settings};
use lol_html::{OutputSink, element, text};

/// What a `decdn:` URI starts with.
const SCHEME: &[u8] = decdn_client::uri::SCHEME.as_bytes();

/// What a rewritten URI starts with.
const SITE_PREFIX: &[u8] = b"/_decdn/";

/// The scheme and the hash: the bytes that make a URI.
const URI_HEAD: usize = SCHEME.len() + 64;

/// The attributes whose values are URLs or carry them: `srcset` lists them,
/// `style` holds CSS, and `content` is where `<meta http-equiv="refresh">`
/// names its target.
const URL_ATTRIBUTES: &[&str] = &[
    "href",
    "src",
    "srcset",
    "action",
    "formaction",
    "poster",
    "data",
    "background",
    "style",
    "content",
];

/// A streaming rewrite of one response body.
pub(super) enum Rewrite {
    /// A `text/html` body, through `lol_html`.
    Html(Box<HtmlRewrite>),
    /// A `text/css` body, scanned as bytes.
    Css(CssRewrite),
}

impl Rewrite {
    /// The rewrite for a body of MIME type `essence`, or `None` for a type
    /// that carries no URIs the gateway rewrites.
    pub(super) fn for_type(essence: &str) -> Option<Self> {
        match essence {
            "text/html" => Some(Self::Html(Box::new(HtmlRewrite::new()))),
            "text/css" => Some(Self::Css(CssRewrite::default())),
            _ => None,
        }
    }

    /// Rewrite the next chunk of the body: the output it lets out so far.
    pub(super) fn feed(&mut self, chunk: &[u8]) -> std::io::Result<Bytes> {
        match self {
            Self::Html(html) => html.feed(chunk),
            Self::Css(css) => Ok(Bytes::from(css.feed(chunk))),
        }
    }

    /// The output the body's end lets out.
    pub(super) fn finish(self) -> std::io::Result<Bytes> {
        match self {
            Self::Html(html) => html.finish(),
            Self::Css(css) => Ok(Bytes::from(css.finish())),
        }
    }
}

/// `s` with every `decdn:` URI rewritten, or `None` when it has none.
pub(super) fn rewrite_str(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    scan(s.as_bytes(), None, s.len(), &mut out);
    if out == s.as_bytes() {
        return None;
    }
    // Only ASCII is replaced, so the output stays UTF-8.
    String::from_utf8(out).ok()
}

/// Copy `input` into `out` from its start up to `end`, rewriting each URI
/// that starts before `end`, and return how far it read. `prev` is the byte
/// before `input`. A URI starting before `end` needs [`URI_HEAD`] bytes from
/// its start to be recognized, so the caller holds those back until the
/// input ends.
fn scan(input: &[u8], prev: Option<u8>, end: usize, out: &mut Vec<u8>) -> usize {
    let mut at = 0;
    while at < end {
        if starts_uri(input, at, prev) {
            out.extend_from_slice(SITE_PREFIX);
            at += SCHEME.len();
            continue;
        }
        if let Some(&b) = input.get(at) {
            out.push(b);
        }
        at += 1;
    }
    at
}

/// Whether a `decdn:` URI starts at `input[at]`.
fn starts_uri(input: &[u8], at: usize, prev: Option<u8>) -> bool {
    let before = match at.checked_sub(1) {
        Some(i) => input.get(i).copied(),
        None => prev,
    };
    // `xdecdn:` names another scheme.
    if before.is_some_and(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b)) {
        return false;
    }
    input.get(at..at + SCHEME.len()) == Some(SCHEME)
        && input
            .get(at + SCHEME.len()..at + URI_HEAD)
            .is_some_and(|hash| hash.iter().all(u8::is_ascii_hexdigit))
}

/// The CSS rewrite: a byte scan that holds back the tail a URI could still
/// start in.
#[derive(Default)]
pub(super) struct CssRewrite {
    pending: Vec<u8>,
    prev: Option<u8>,
}

impl CssRewrite {
    fn feed(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.pending.extend_from_slice(chunk);
        let end = self.pending.len().saturating_sub(URI_HEAD - 1);
        self.drain(end)
    }

    fn finish(mut self) -> Vec<u8> {
        let end = self.pending.len();
        self.drain(end)
    }

    fn drain(&mut self, end: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(end);
        let read = scan(&self.pending, self.prev, end, &mut out).min(self.pending.len());
        if let Some(last) = read.checked_sub(1) {
            self.prev = self.pending.get(last).copied();
        }
        self.pending.drain(..read);
        out
    }
}

/// Where `lol_html` writes its output, for [`HtmlRewrite`] to hand on.
#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Output {
    fn take(&self) -> Bytes {
        Bytes::from(std::mem::take(
            &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner),
        ))
    }
}

impl OutputSink for Output {
    fn handle_chunk(&mut self, chunk: &[u8]) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(chunk);
    }
}

/// The HTML rewrite.
pub(super) struct HtmlRewrite {
    /// Behind a mutex only so a response body holding it is `Sync`; every
    /// access is through `&mut self`, so it is never locked.
    rewriter: Mutex<HtmlRewriter<'static, Output>>,
    output: Output,
}

impl HtmlRewrite {
    fn new() -> Self {
        let output = Output::default();
        // A `<style>` element's text arrives in chunks a URI can straddle, so
        // it is gathered and rewritten whole.
        let mut style = String::new();
        let settings = Settings::new_send()
            .append_element_content_handler(element!("*", |el| {
                for name in URL_ATTRIBUTES {
                    if let Some(value) = el.get_attribute(name)
                        && let Some(rewritten) = rewrite_str(&value)
                    {
                        el.set_attribute(name, &rewritten)?;
                    }
                }
                Ok(())
            }))
            .append_element_content_handler(text!("style", move |chunk| {
                style.push_str(chunk.as_str());
                if chunk.last_in_text_node() {
                    let css = std::mem::take(&mut style);
                    chunk.replace(&rewrite_str(&css).unwrap_or(css), ContentType::Html);
                } else {
                    chunk.remove();
                }
                Ok(())
            }));
        Self {
            rewriter: Mutex::new(HtmlRewriter::new(settings, output.clone())),
            output,
        }
    }

    fn feed(&mut self, chunk: &[u8]) -> std::io::Result<Bytes> {
        self.rewriter
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .write(chunk)
            .map_err(std::io::Error::other)?;
        Ok(self.output.take())
    }

    fn finish(self) -> std::io::Result<Bytes> {
        let Self { rewriter, output } = self;
        rewriter
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .end()
            .map_err(std::io::Error::other)?;
        Ok(output.take())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests;
