//! The requester side of `APP_ERR_RATE_LIMITED` (ADR 013 §Application Error
//! Codes).
//!
//! A node that sheds load at the transport closes the connection (or resets the
//! stream) with `0x10` before any application message exists, so nothing signed
//! ever says "overloaded". The code and its layer label are the only evidence
//! the requester gets, and they arrive buried in a `ConnectionError`, a
//! `ReadError`, or a `WriteError` nested inside whichever transport call failed
//! first. [`rate_limit_shed`] digs that out and [`UpstreamRateLimited`] carries
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
/// Built only by [`rate_limit_shed`]; the fields are public so a consumer can
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
pub fn rate_limit_shed(err: &(dyn std::error::Error + 'static)) -> Option<UpstreamRateLimited> {
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
mod tests {
    use super::*;
    use bytes::Bytes;
    use decdn_protocol::FrameError;

    fn app_close(code: u32, reason: &'static [u8]) -> ConnectionError {
        ConnectionError::ApplicationClosed(ApplicationClose {
            error_code: VarInt::from_u32(code),
            reason: Bytes::from_static(reason),
        })
    }

    #[test]
    fn a_rate_limit_connection_close_carries_its_layer_label() {
        let shed = rate_limit_shed(&app_close(APP_ERR_RATE_LIMITED, b"per-source"));
        assert_eq!(
            shed,
            Some(UpstreamRateLimited {
                label: Some("per-source".to_owned())
            })
        );
    }

    #[test]
    fn a_close_with_another_code_is_not_a_shed() {
        assert_eq!(rate_limit_shed(&app_close(0x00, b"idle")), None);
        assert_eq!(rate_limit_shed(&app_close(0x03, b"malformed")), None);
        assert_eq!(rate_limit_shed(&ConnectionError::TimedOut), None);
    }

    /// The stream-cap reset has no reason bytes, so the label is `None`, and the
    /// code still has to be the rate-limit one.
    #[test]
    fn a_rate_limit_stream_reset_or_stop_has_no_label() {
        let reset = ReadError::Reset(VarInt::from_u32(APP_ERR_RATE_LIMITED));
        assert_eq!(
            rate_limit_shed(&reset),
            Some(UpstreamRateLimited { label: None })
        );
        let stopped = WriteError::Stopped(VarInt::from_u32(APP_ERR_RATE_LIMITED));
        assert_eq!(
            rate_limit_shed(&stopped),
            Some(UpstreamRateLimited { label: None })
        );
        assert_eq!(
            rate_limit_shed(&ReadError::Reset(VarInt::from_u32(0x03))),
            None
        );
        assert_eq!(
            rate_limit_shed(&WriteError::Stopped(VarInt::from_u32(0x03))),
            None
        );
    }

    /// What `read_frame` / `write_frame` actually return: the QUIC stream error
    /// wrapped as an `io::Error`'s inner error, inside `FrameError::Io`. Both the
    /// bare reset (reachable only via `get_ref`) and the connection-lost close
    /// (reachable via `source`) must be found through that nesting.
    #[test]
    fn a_shed_is_found_through_frame_error_and_io_error_nesting() {
        let reset: FrameError =
            std::io::Error::from(ReadError::Reset(VarInt::from_u32(APP_ERR_RATE_LIMITED))).into();
        assert_eq!(
            rate_limit_shed(&reset),
            Some(UpstreamRateLimited { label: None })
        );

        let lost: FrameError = std::io::Error::from(ReadError::ConnectionLost(app_close(
            APP_ERR_RATE_LIMITED,
            b"global-full",
        )))
        .into();
        assert_eq!(
            rate_limit_shed(&lost),
            Some(UpstreamRateLimited {
                label: Some("global-full".to_owned())
            })
        );

        let stopped: FrameError = std::io::Error::from(WriteError::ConnectionLost(app_close(
            APP_ERR_RATE_LIMITED,
            b"per_peer",
        )))
        .into();
        assert_eq!(
            rate_limit_shed(&stopped),
            Some(UpstreamRateLimited {
                label: Some("per_peer".to_owned())
            })
        );

        let plain: FrameError = std::io::Error::from(ReadError::ClosedStream).into();
        assert_eq!(rate_limit_shed(&plain), None);
        assert_eq!(rate_limit_shed(&FrameError::Varint), None);
    }

    /// The orchestrator's `downcast_ref` must recover the sentinel through the
    /// stage context this helper adds and any context a caller adds on top.
    #[test]
    fn transport_error_types_a_shed_and_keeps_plain_text_otherwise() -> anyhow::Result<()> {
        let shed = transport_error(
            "open_bi failed",
            app_close(APP_ERR_RATE_LIMITED, b"per-source"),
        )
        .context("probe candidate");
        let sentinel = shed
            .downcast_ref::<UpstreamRateLimited>()
            .ok_or_else(|| anyhow::anyhow!("sentinel lost: {shed:#}"))?;
        assert_eq!(sentinel.label.as_deref(), Some("per-source"));

        let plain = transport_error("open_bi failed", ConnectionError::TimedOut);
        assert!(plain.downcast_ref::<UpstreamRateLimited>().is_none());
        assert_eq!(plain.to_string(), "open_bi failed: timed out");
        Ok(())
    }
}
