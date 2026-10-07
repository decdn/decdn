//! The requester side of `APP_ERR_RATE_LIMITED` (ADR 013 §Application Error
//! Codes).
//!
//! A node that sheds load at the transport closes the connection (or resets the
//! stream) with `0x10` before any application message exists, so nothing signed
//! ever says "overloaded". The code and its layer label are the only evidence
//! the requester gets, and they arrive buried in a `ConnectionError`, a
//! `ReadError`, or a `WriteError` nested inside whichever transport call failed
//! first. `rate_limit_shed` digs that out and [`UpstreamRateLimited`] carries
//! it as a typed sentinel the pull orchestrator can `downcast_ref`, so a shed
//! reads as what it is — a reachable peer refusing work — rather than as a dead
//! peer.

use std::fmt;

use decdn_protocol::APP_ERR_RATE_LIMITED;
use iroh::endpoint::{ApplicationClose, ConnectionError, ReadError, VarInt, WriteError};

/// Typed sentinel for a peer shedding this connection or stream with
/// `APP_ERR_RATE_LIMITED` (`0x10`).
///
/// Proof the peer is reachable and answering — it chose to refuse work, it did
/// not fail to receive it — so it is no evidence of degradation. A consumer
/// treats it the way it treats the handler-level `StreamError::Overloaded`:
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
    let mut cursor: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cursor {
        if let Some(hit) = shed_at(e) {
            return Some(hit);
        }
        cursor = e.source();
    }
    None
}

/// Match one hop of the chain. The `io::Error` arm recurses into the wrapped
/// error, which is itself a `ReadError` / `WriteError` whose `ConnectionLost`
/// source may be the close.
fn shed_at(e: &(dyn std::error::Error + 'static)) -> Option<UpstreamRateLimited> {
    if let Some(conn) = e.downcast_ref::<ConnectionError>() {
        return closed_by_rate_limit(conn);
    }
    if let Some(read) = e.downcast_ref::<ReadError>() {
        return match read {
            ReadError::Reset(code) if is_rate_limit(*code) => {
                Some(UpstreamRateLimited { label: None })
            }
            ReadError::ConnectionLost(conn) => closed_by_rate_limit(conn),
            _ => None,
        };
    }
    if let Some(write) = e.downcast_ref::<WriteError>() {
        return match write {
            WriteError::Stopped(code) if is_rate_limit(*code) => {
                Some(UpstreamRateLimited { label: None })
            }
            WriteError::ConnectionLost(conn) => closed_by_rate_limit(conn),
            _ => None,
        };
    }
    if let Some(io) = e.downcast_ref::<std::io::Error>() {
        return io.get_ref().and_then(|inner| rate_limit_shed(inner));
    }
    None
}

fn closed_by_rate_limit(conn: &ConnectionError) -> Option<UpstreamRateLimited> {
    match conn {
        ConnectionError::ApplicationClosed(ApplicationClose { error_code, reason })
            if is_rate_limit(*error_code) =>
        {
            Some(UpstreamRateLimited {
                label: Some(String::from_utf8_lossy(reason).into_owned()),
            })
        }
        _ => None,
    }
}

fn is_rate_limit(code: VarInt) -> bool {
    code == VarInt::from_u32(APP_ERR_RATE_LIMITED)
}

/// Convert a failed transport call into the error the requester returns.
///
/// A `0x10` shed becomes the [`UpstreamRateLimited`] sentinel with `stage` as
/// context, so `downcast_ref` recovers it through any further context a caller
/// adds. Everything else keeps the plain `"{stage}: {err}"` text that logs and
/// the loopback tests match on.
pub(crate) fn transport_error<E>(stage: &'static str, err: E) -> anyhow::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    match rate_limit_shed(&err) {
        Some(shed) => anyhow::Error::new(shed).context(stage),
        None => anyhow::anyhow!("{stage}: {err}"),
    }
}

#[cfg(test)]
mod tests;
