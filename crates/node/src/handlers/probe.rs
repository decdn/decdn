//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use decdn_protocol::{
    ALPN_PROBE,
    message::{ProbeRequest, ProbeResponse},
};
use iroh::{PublicKey, endpoint::Connection};

use super::Handler;
use crate::metrics::Metrics;

const MAX_REQUEST_BYTES: usize = 64;
/// Ceiling on how long we wait for the client to send the `ProbeRequest` and
/// FIN the stream. Without it a peer can pin a server task indefinitely by
/// opening a stream and never closing it.
const PROBE_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Ceiling on how long we wait for the client to close the connection after
/// receiving the response. Bounds the `active_connections` gauge against idle
/// clients that hold the connection open.
const PROBE_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serves `cdn/probe/v1`: reads a [`ProbeRequest`], writes a [`ProbeResponse`].
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
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .map_err(|e| anyhow::anyhow!("accept_bi failed: {e}"))?;

        let buf = tokio::time::timeout(PROBE_READ_TIMEOUT, recv.read_to_end(MAX_REQUEST_BYTES))
            .await
            .map_err(|_| anyhow::anyhow!("probe request timed out after {PROBE_READ_TIMEOUT:?}"))?
            .map_err(|e| anyhow::anyhow!("probe request read failed: {e}"))?;

        let req: ProbeRequest =
            postcard::from_bytes(&buf).map_err(|e| anyhow::anyhow!("probe decode failed: {e}"))?;

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

        let bytes = postcard::to_allocvec(&resp)
            .map_err(|e| anyhow::anyhow!("probe encode failed: {e}"))?;

        send.write_all(&bytes)
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
