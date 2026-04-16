//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use decdn_protocol::{
    ALPN_PROBE, FrameError, ProbeMessage, decode_message, encode_message, message::ProbeResponse,
    read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::metrics::Metrics;

/// Ceiling on how long we wait for the client to open the bi-directional
/// stream. Without it a peer can sit on an accepted connection without ever
/// opening a stream.
const ACCEPT_BI_TIMEOUT: Duration = Duration::from_secs(5);
/// Ceiling on how long we wait for the client to send the framed request.
/// Without it a peer can pin a server task indefinitely.
const PROBE_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Ceiling on how long we wait for the client to close the connection after
/// receiving the response.
const PROBE_CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

// QUIC application error codes defined by ADR 013 §Application Error Codes.
const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;
const APP_ERR_MESSAGE_TOO_LARGE: u32 = 0x02;
const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

/// Serves `cdn/probe/v1`: reads a framed [`ProbeMessage::Request`], writes a
/// framed [`ProbeMessage::Response`].
#[derive(Debug)]
pub struct ProbeHandler {
    node_id: PublicKey,
    rate_per_mb: u64,
    metrics: Arc<Metrics>,
}

impl ProbeHandler {
    pub const ALPN: &'static [u8] = ALPN_PROBE;

    #[allow(clippy::missing_const_for_fn)] // Arc::new isn't const.
    pub fn new(node_id: PublicKey, rate_per_mb: u64, metrics: Arc<Metrics>) -> Self {
        Self {
            node_id,
            rate_per_mb,
            metrics,
        }
    }

    async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        let _guard = self.metrics.connection_guard();

        let (mut send, mut recv) = tokio::time::timeout(ACCEPT_BI_TIMEOUT, conn.accept_bi())
            .await
            .map_err(|_| anyhow::anyhow!("accept_bi timed out after {ACCEPT_BI_TIMEOUT:?}"))?
            .map_err(|e| anyhow::anyhow!("accept_bi failed: {e}"))?;

        let req = match read_probe_request(&mut send, &mut recv).await {
            Ok(req) => req,
            Err(ProbeReadError { err, app_code }) => {
                // ADR 013 scopes app error codes to streams, but probe is 1:1
                // connection:stream — also close the connection with the same
                // code so the peer observes it deterministically even if the
                // stream RESET racing with connection teardown gets clobbered.
                conn.close(VarInt::from_u32(app_code), b"probe-error");
                return Err(err);
            }
        };

        let measured_at_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX);

        let resp = ProbeResponse {
            nonce: req.nonce,
            measured_at_unix_ms,
            node_id: *self.node_id.as_bytes(),
            rate_per_mb: self.rate_per_mb,
        };

        let payload = encode_message(&ProbeMessage::Response(resp))
            .map_err(|e| anyhow::anyhow!("probe encode failed: {e}"))?;
        write_frame(&mut send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("probe response write failed: {e}"))?;
        send.finish()
            .map_err(|e| anyhow::anyhow!("probe stream finish failed: {e}"))?;

        self.metrics.probe_request();
        // Wait for the client's close so the response bytes are flushed to the
        // peer, but cap the wait so an idle/malicious client can't hold the
        // connection (and inflate active_connections) forever.
        let _ = tokio::time::timeout(PROBE_CLOSE_TIMEOUT, conn.closed()).await;
        conn.close(0u32.into(), b"probe-done");
        Ok(())
    }
}

impl ProtocolHandler for ProbeHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.serve(connection)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}

const fn frame_err_code(e: &FrameError) -> u32 {
    match e {
        FrameError::TooLarge(_) => APP_ERR_MESSAGE_TOO_LARGE,
        _ => APP_ERR_MALFORMED_MESSAGE,
    }
}

/// Error from the probe-request read path carrying the ADR 013 app error
/// code the handler should propagate to the peer.
struct ProbeReadError {
    err: anyhow::Error,
    app_code: u32,
}

/// Reads one framed `ProbeMessage::Request` from `recv`. On failure, also
/// resets/stops the streams with the appropriate ADR 013 app error code so
/// long-lived (future) multi-stream connections can keep running; the caller
/// additionally closes the whole connection for probe's 1:1 topology.
///
/// The cognitive-complexity allowance reflects that splitting this further
/// would spread the ADR 013 error-code mapping across multiple helpers,
/// making it harder to audit against the spec.
#[allow(clippy::cognitive_complexity)]
async fn read_probe_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
) -> Result<decdn_protocol::message::ProbeRequest, ProbeReadError> {
    let reset = |send: &mut SendStream, recv: &mut RecvStream, code: u32| {
        let v = VarInt::from_u32(code);
        // reset/stop may fail if stream already closed by peer; ignore.
        let _ = send.reset(v);
        let _ = recv.stop(v);
    };

    let frame = match tokio::time::timeout(PROBE_READ_TIMEOUT, read_frame(recv)).await {
        Err(_) => {
            // ADR 013 defines no timeout-specific code; use 0 (no app error).
            reset(send, recv, 0);
            tracing::warn!(
                timeout_ms = u64::try_from(PROBE_READ_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                "probe request read timed out"
            );
            return Err(ProbeReadError {
                err: anyhow::anyhow!("probe request timed out after {PROBE_READ_TIMEOUT:?}"),
                app_code: 0,
            });
        }
        Ok(Err(e)) => {
            let app_code = frame_err_code(&e);
            reset(send, recv, app_code);
            tracing::warn!(app_code, error = %e, "probe frame read failed");
            return Err(ProbeReadError {
                err: anyhow::anyhow!("probe frame read failed: {e}"),
                app_code,
            });
        }
        Ok(Ok(frame)) => frame,
    };

    match decode_message::<ProbeMessage>(&frame) {
        Err(e) => {
            reset(send, recv, APP_ERR_MALFORMED_MESSAGE);
            tracing::warn!(
                app_code = APP_ERR_MALFORMED_MESSAGE,
                error = %e,
                "probe message decode failed"
            );
            Err(ProbeReadError {
                err: anyhow::anyhow!("probe decode failed: {e}"),
                app_code: APP_ERR_MALFORMED_MESSAGE,
            })
        }
        Ok((ProbeMessage::Request(req), _rest)) => Ok(req),
        Ok((ProbeMessage::Response(_), _)) => {
            reset(send, recv, APP_ERR_UNSUPPORTED_MESSAGE);
            tracing::warn!(
                app_code = APP_ERR_UNSUPPORTED_MESSAGE,
                "peer sent ProbeMessage::Response on server stream"
            );
            Err(ProbeReadError {
                err: anyhow::anyhow!("unexpected ProbeMessage::Response on server stream"),
                app_code: APP_ERR_UNSUPPORTED_MESSAGE,
            })
        }
    }
}
