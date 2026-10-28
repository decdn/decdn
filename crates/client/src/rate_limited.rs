//! The requester side of the unsigned transport refusal codes
//! `APP_ERR_RATE_LIMITED` and `APP_ERR_TIMESTAMP_SKEW` (ADR 013 §Application
//! Error Codes).
//!
//! A node that refuses at the transport closes the connection (or resets the
//! stream) with the code before it sends any application message, so nothing
//! signed ever says "overloaded" or "clock skewed". The code (and, on a
//! connection close, its reason label) is the only evidence the requester
//! gets, and it arrives buried in a `ConnectionError`, a `ReadError`, or a
//! `WriteError` nested inside whichever transport call failed first.
//! `rate_limit_shed` / `timestamp_rejected` dig that out, and
//! [`UpstreamRateLimited`] / [`UpstreamTimestampRejected`] carry it as typed
//! sentinels a consumer can `downcast_ref`, so a shed reads as a reachable
//! peer refusing work and a timestamp refusal names this machine's clock,
//! rather than either reading as a dead peer. Any other code stays a plain
//! transport error.

use std::fmt;

use decdn_protocol::{APP_ERR_RATE_LIMITED, APP_ERR_TIMESTAMP_SKEW};
use iroh::endpoint::{ApplicationClose, ConnectionError, ReadError, VarInt, WriteError};

/// Typed sentinel for a peer shedding this connection or stream with
/// `APP_ERR_RATE_LIMITED` (`0x10`).
///
/// Proof the peer is reachable and answering — it chose to refuse work, it did
/// not fail to receive it — so it is no evidence of degradation. A consumer
/// treats it the way it treats a handler-level `StreamError::NotFound`:
/// suppress the `(peer, hash)` pair briefly and record no reputation outcome.
/// Backpressure is respected, never punished (ADR 041 §Refusing is not
/// slashable; ADR 013 says a peer receiving `0x10` MUST NOT treat it as a
/// protocol error).
///
/// Built only by `rate_limit_shed`; the fields are public so a consumer can
/// log which layer shed and a test can assert on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamRateLimited {
    /// The layer label the node put in its `CONNECTION_CLOSE` reason bytes
    /// (`global-full`, `per-source`, `per_peer`, …), lossily decoded. `None`
    /// when the shed was a `RESET_STREAM`, which carries no reason bytes.
    pub label: Option<String>,
}

impl fmt::Display for UpstreamRateLimited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.label {
            Some(label) => write!(
                f,
                "upstream rate-limited (connection close, layer {label:?})"
            ),
            None => write!(f, "upstream rate-limited (stream reset)"),
        }
    }
}

impl std::error::Error for UpstreamRateLimited {}

/// Typed sentinel for a peer refusing this request's timestamp with
/// `APP_ERR_TIMESTAMP_SKEW` (`0x04`): the request's `timestamp_us` sat more
/// than 30 seconds from the node's clock, so it refused before signing
/// anything (ADR 013 §Application Error Codes). The usual cause is this
/// machine's clock. If only one peer refuses, that peer's clock is wrong. So
/// the `Display` text says to check the clock, and the failure does not read
/// as a dead peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTimestampRejected {
    /// The reason label from the `CONNECTION_CLOSE` frame (`cdn/probe/v1`
    /// mirrors the stream code there), lossily decoded. `None` when the
    /// refusal was a `RESET_STREAM`, which carries no reason bytes.
    pub label: Option<String>,
}

impl fmt::Display for UpstreamTimestampRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "upstream rejected this request's timestamp as too far from its clock; \
             check this machine's clock",
        )
    }
}

impl std::error::Error for UpstreamTimestampRejected {}

/// Recover a `0x10` shed from anywhere in `err`'s source chain.
///
/// Walks `err` and every `source()` below it, and at each hop recognises the
/// three shapes a shed reaches the requester in:
///
/// - `ConnectionError::ApplicationClosed` with the rate-limit code — the close
///   the dispatch limiter and the probe limiter send, surfacing from `connect`,
///   `open_bi`, or as the `ConnectionLost` cause of a stream read/write.
/// - `ReadError::Reset` / `WriteError::Stopped` with the rate-limit code — the
///   per-connection stream-cap reset.
/// - `std::io::Error` — what `read_frame` / `write_frame` see, since the QUIC
///   stream errors above are wrapped as the io error's *inner* error. That
///   inner error is reached through `get_ref()`, not `source()`: `io::Error`'s
///   `source()` skips the wrapped error and returns *its* source, which for a
///   bare `Reset` is nothing.
///
/// Any other code, and any chain with no transport error in it, is `None`.
#[must_use]
pub(crate) fn rate_limit_shed(
    err: &(dyn std::error::Error + 'static),
) -> Option<UpstreamRateLimited> {
    app_code_refusal(err, APP_ERR_RATE_LIMITED)
        .map(|RefusalLabel(label)| UpstreamRateLimited { label })
}

/// Recover a `0x04` timestamp refusal from anywhere in `err`'s source chain —
/// the `TIMESTAMP_SKEW` twin of [`rate_limit_shed`].
#[must_use]
pub(crate) fn timestamp_rejected(
    err: &(dyn std::error::Error + 'static),
) -> Option<UpstreamTimestampRejected> {
    app_code_refusal(err, APP_ERR_TIMESTAMP_SKEW)
        .map(|RefusalLabel(label)| UpstreamTimestampRejected { label })
}

/// A matched refusal's close-frame label: the reason bytes of a connection
/// close, or `None` inside for a stream reset, which carries none. A newtype
/// rather than a bare `Option<String>`, so "found with no label" and "not
/// found" stay distinct types in the walkers below.
struct RefusalLabel(Option<String>);

/// Find `code` as an application-level refusal anywhere in `err`'s source
/// chain. Any other code, and any chain with no transport error in it, is
/// `None`.
fn app_code_refusal(err: &(dyn std::error::Error + 'static), code: u32) -> Option<RefusalLabel> {
    let mut cursor: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cursor {
        if let Some(hit) = code_at(e, code) {
            return Some(hit);
        }
        cursor = e.source();
    }
    None
}

/// Match one hop of the chain against `code`. The `io::Error` arm recurses
/// into the wrapped error, which is itself a `ReadError` / `WriteError` whose
/// `ConnectionLost` source may be the close. The three shapes a refusal
/// reaches the requester in:
///
/// - `ConnectionError::ApplicationClosed` with the code — the close the
///   dispatch limiter, the probe limiter and the probe skew gate send,
///   surfacing from `connect`, `open_bi`, or as the `ConnectionLost` cause of
///   a stream read/write.
/// - `ReadError::Reset` / `WriteError::Stopped` with the code — the
///   per-connection stream-cap reset and the stream skew gate's reset.
/// - `std::io::Error` — what `read_frame` / `write_frame` see, since the QUIC
///   stream errors above are wrapped as the io error's *inner* error. That
///   inner error is reached through `get_ref()`, not `source()`: `io::Error`'s
///   `source()` skips the wrapped error and returns *its* source, which for a
///   bare `Reset` is nothing.
fn code_at(e: &(dyn std::error::Error + 'static), code: u32) -> Option<RefusalLabel> {
    if let Some(conn) = e.downcast_ref::<ConnectionError>() {
        return closed_with_code(conn, code);
    }
    if let Some(read) = e.downcast_ref::<ReadError>() {
        return match read {
            ReadError::Reset(got) if *got == VarInt::from_u32(code) => Some(RefusalLabel(None)),
            ReadError::ConnectionLost(conn) => closed_with_code(conn, code),
            _ => None,
        };
    }
    if let Some(write) = e.downcast_ref::<WriteError>() {
        return match write {
            WriteError::Stopped(got) if *got == VarInt::from_u32(code) => Some(RefusalLabel(None)),
            WriteError::ConnectionLost(conn) => closed_with_code(conn, code),
            _ => None,
        };
    }
    if let Some(io) = e.downcast_ref::<std::io::Error>() {
        return io.get_ref().and_then(|inner| app_code_refusal(inner, code));
    }
    None
}

fn closed_with_code(conn: &ConnectionError, code: u32) -> Option<RefusalLabel> {
    match conn {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, reason })
            if *error_code == VarInt::from_u32(code) =>
        {
            Some(RefusalLabel(Some(
                String::from_utf8_lossy(reason).into_owned(),
            )))
        }
        _ => None,
    }
}

/// Convert a failed transport call into the error the requester returns.
///
/// A `0x10` shed becomes the [`UpstreamRateLimited`] sentinel and a `0x04`
/// timestamp refusal the [`UpstreamTimestampRejected`] sentinel, each with
/// `stage` as context, so `downcast_ref` recovers them through any further
/// context a caller adds. Everything else — unknown codes included — keeps
/// the plain `"{stage}: {err}"` text that logs and the loopback tests match
/// on.
pub(crate) fn transport_error<E>(stage: &'static str, err: E) -> anyhow::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    if let Some(shed) = rate_limit_shed(&err) {
        return anyhow::Error::new(shed).context(stage);
    }
    if let Some(rejected) = timestamp_rejected(&err) {
        return anyhow::Error::new(rejected).context(stage);
    }
    anyhow::anyhow!("{stage}: {err}")
}

#[cfg(test)]
mod tests;
