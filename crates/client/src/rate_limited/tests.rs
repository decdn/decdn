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
