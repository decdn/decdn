//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use decdn_protocol::{
    ALPN_PROBE, FrameError, ProbeMessage, decode_message, encode_message, message::ProbeResponse,
    read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Connection, VarInt};

use super::Handler;
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
    #[allow(clippy::missing_const_for_fn)] // Arc::new isn't const.
    pub fn new(node_id: PublicKey, rate_per_mb: u64, metrics: Arc<Metrics>) -> Self {
        Self {
            node_id,
            rate_per_mb,
            metrics,
        }
    }

    async fn serve(self: Arc<Self>, conn: Connection) -> anyhow::Result<()> {
        let (mut send, mut recv) = tokio::time::timeout(ACCEPT_BI_TIMEOUT, conn.accept_bi())
            .await
            .map_err(|_| anyhow::anyhow!("accept_bi timed out after {ACCEPT_BI_TIMEOUT:?}"))?
            .map_err(|e| anyhow::anyhow!("accept_bi failed: {e}"))?;

        let frame_result = tokio::time::timeout(PROBE_READ_TIMEOUT, read_frame(&mut recv)).await;
        let req = match frame_result {
            Err(_) => {
                // ADR 013 defines no timeout-specific code; use 0 (no app error)
                // and let the connection teardown signal the peer.
                let code = VarInt::from_u32(0);
                let _ = send.reset(code);
                let _ = recv.stop(code);
                return Err(anyhow::anyhow!(
                    "probe request timed out after {PROBE_READ_TIMEOUT:?}"
                ));
            }
            Ok(Err(e)) => {
                let code = VarInt::from_u32(frame_err_code(&e));
                let _ = send.reset(code);
                let _ = recv.stop(code);
                return Err(anyhow::anyhow!("probe frame read failed: {e}"));
            }
            Ok(Ok(frame)) => match decode_message::<ProbeMessage>(&frame) {
                Err(e) => {
                    let code = VarInt::from_u32(APP_ERR_MALFORMED_MESSAGE);
                    let _ = send.reset(code);
                    let _ = recv.stop(code);
                    return Err(anyhow::anyhow!("probe decode failed: {e}"));
                }
                Ok((ProbeMessage::Request(req), _rest)) => req,
                Ok((ProbeMessage::Response(_), _)) => {
                    let code = VarInt::from_u32(APP_ERR_UNSUPPORTED_MESSAGE);
                    let _ = send.reset(code);
                    let _ = recv.stop(code);
                    return Err(anyhow::anyhow!(
                        "unexpected ProbeMessage::Response on server stream"
                    ));
                }
            },
        };

        let measured_at_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
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

const fn frame_err_code(e: &FrameError) -> u32 {
    match e {
        FrameError::TooLarge(_) => APP_ERR_MESSAGE_TOO_LARGE,
        _ => APP_ERR_MALFORMED_MESSAGE,
    }
}

impl Handler for ProbeHandler {
    fn alpn(&self) -> &'static [u8] {
        ALPN_PROBE
    }

    fn handle(
        self: Arc<Self>,
        conn: Connection,
    ) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>> {
        Box::pin(self.serve(conn))
    }
}
