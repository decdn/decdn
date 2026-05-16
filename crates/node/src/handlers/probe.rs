//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use decdn_protocol::{
    ALPN_PROBE, APP_ERR_RATE_LIMITED, FrameError, ProbeMessage, decode_message, encode_message,
    message::ProbeResponse, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Accepting, Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;

// Server-side timeouts. Each ceiling exists so a single peer cannot pin a
// handler task indefinitely by stalling at one of the protocol's ordered
// steps.
const ACCEPT_BI_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_READ_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
/// Bound on the post-rejection close-frame flush. The QUIC `CONNECTION_CLOSE`
/// frame is best-effort; we wait briefly for the peer to acknowledge so the
/// `0x10 RATE_LIMITED` reason byte (and its layer label) reach them, but cap
/// the wait so a malicious flooder can't keep the handler alive by refusing
/// to acknowledge.
const REJECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

// QUIC application error codes defined by ADR 013 §Application Error Codes.
const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;
const APP_ERR_MESSAGE_TOO_LARGE: u32 = 0x02;
const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

/// Serves `cdn/probe/v1`: reads a framed [`ProbeMessage::Request`], writes a
/// framed [`ProbeMessage::Response`].
///
/// `rate_per_mb` is held behind a shared `AtomicU64` so config reload can
/// swap the value without rebuilding the handler or touching the iroh
/// `Router`. Reads use `Ordering::Relaxed`: the rate is a single-word
/// counter with no ordering relationship to other state, and any in-flight
/// probe simply observes whichever generation of the rate the load happens
/// to see.
pub struct ProbeHandler {
    node_id: PublicKey,
    rate_per_mb: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    /// ADR 015 master switch (`network.enable_0rtt`). When `true`, this
    /// handler overrides `on_accepting` to accept QUIC 0-RTT for
    /// `cdn/probe/v1` (the per-ALPN gate is structural: only handlers
    /// that override `on_accepting` ever surface early data). When
    /// `false`, the default full-handshake `on_accepting` is used and the
    /// connection is served 1-RTT, exactly as before ADR 015.
    enable_0rtt: bool,
}

impl std::fmt::Debug for ProbeHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeHandler")
            .field("node_id", &self.node_id)
            .field("rate_per_mb", &self.rate_per_mb)
            .field("enable_0rtt", &self.enable_0rtt)
            .finish_non_exhaustive()
    }
}

impl ProbeHandler {
    pub const ALPN: &'static [u8] = ALPN_PROBE;

    #[allow(clippy::missing_const_for_fn)] // Arc::new isn't const.
    pub fn new(
        node_id: PublicKey,
        rate_per_mb: Arc<AtomicU64>,
        metrics: Arc<Metrics>,
        limiter: Arc<ConnectionLimiter>,
        enable_0rtt: bool,
    ) -> Self {
        Self {
            node_id,
            rate_per_mb,
            metrics,
            limiter,
            enable_0rtt,
        }
    }

    async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        let _permit = match self.limiter.acquire(&conn) {
            Ok(p) => p,
            Err(reason) => {
                // Rate-limited rejection is normal load-shedding, not a
                // protocol fault: returning `Err` here would have iroh log
                // every rejection as an `AcceptError`, amplifying log
                // volume under flood (exactly what the attacker wants).
                // The dispatch layer already emits a structured debug log
                // and a metric counter for the rejection.
                conn.close(
                    VarInt::from_u32(APP_ERR_RATE_LIMITED),
                    reason.as_str().as_bytes(),
                );
                // Wait for the close frame to be acknowledged so the peer
                // reliably observes the 0x10 RATE_LIMITED code and the
                // layer-label reason byte — except on `GlobalFull`. Under
                // a global-cap flood every rejection would otherwise
                // park a task here for up to `REJECTION_CLOSE_TIMEOUT`,
                // and at thousands of rejections per second that is the
                // memory-pressure path the limiter exists to prevent.
                // Layer label is least useful for `GlobalFull` anyway
                // (operators pivot on the metric counter, not the close
                // reason byte). Per-source rejections are rate-limited
                // by the bucket itself, so the bounded wait is safe.
                if reason != RejectReason::GlobalFull {
                    let _ = tokio::time::timeout(REJECTION_CLOSE_TIMEOUT, conn.closed()).await;
                }
                return Ok(());
            }
        };
        let _guard = self.metrics.connection_guard();

        // ADR 005 caps probe at 1 bidi stream per connection; the transport-level
        // cap is the union across ALPNs, so probe's tighter bound is enforced
        // here by accepting exactly one stream and then closing.
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
            rate_per_mb: self.rate_per_mb.load(Ordering::Relaxed),
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
    /// ADR 015 §Server-Side Controls. Overriding `on_accepting` is what
    /// makes `cdn/probe/v1` 0-RTT-eligible — handlers that keep the
    /// default (gossip, future client/dht) await the full handshake and
    /// the QUIC stack drops any early data, so the per-ALPN guarantee is
    /// structural, not a TLS knob.
    ///
    /// `Accepting::into_0rtt()` accepts the client's early data when a
    /// resumption ticket is present (0-RTT) and enables 0.5-RTT
    /// otherwise. When the client did send early data (it had a cached
    /// ticket), its request rode with the `ClientHello` and the handshake
    /// round trip is saved; a cold client is an ordinary 1-RTT connection.
    /// We resolve the connection via `handshake_completed()` and then
    /// serve it through the unchanged 1-RTT `serve()` path: any latency
    /// win is already banked client-side, and serving post-handshake
    /// keeps the limiter, metrics, and ADR 013 error mapping operating on
    /// a connection with a known, authenticated peer. A completed
    /// handshake also means the server has emitted its `NewSessionTicket`
    /// (rustls default `send_tls13_tickets > 0` — a dependency default,
    /// not a protocol guarantee), so the peer is counted toward the
    /// approximate session-ticket gauge.
    ///
    /// When the master switch is off, fall back to the default behavior
    /// (await the full handshake) so the node is byte-for-byte the
    /// pre-ADR-015 1-RTT server.
    async fn on_accepting(&self, accepting: Accepting) -> Result<Connection, AcceptError> {
        if !self.enable_0rtt {
            // `ConnectingError` implements `std::error::Error`; pass it to
            // `AcceptError::from_err` directly (no `to_string()` flatten)
            // so iroh's warn-on-drop log keeps the typed cause chain — the
            // same fidelity the default `on_accepting` (`accepting.await?`)
            // would have produced.
            return accepting.await.map_err(AcceptError::from_err);
        }
        let zrtt = accepting.into_0rtt();
        let conn = zrtt
            .handshake_completed()
            .await
            .map_err(AcceptError::from_err)?;
        // Approximate `quic_session_ticket_cache_size` (ADR 015
        // §Observability). This counts every distinct peer that completed
        // a probe handshake on the 0-RTT-enabled path — cold (no ticket
        // presented) included — not only peers that actually resumed: the
        // server issues a `NewSessionTicket` on each handshake (rustls
        // default `send_tls13_tickets > 0`), so the peer becomes
        // resumption-capable regardless. It is therefore an upper bound on
        // live cached tickets, as the gauge's docs state. Idempotent per
        // peer.
        self.metrics
            .note_session_ticket_peer(*conn.remote_id().as_bytes());
        Ok(conn)
    }

    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.serve(connection)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}

const fn frame_err_code(e: &FrameError) -> u32 {
    // Match every variant explicitly so a future `#[non_exhaustive]` /
    // new variant fails the build instead of being silently collapsed
    // into `APP_ERR_MALFORMED_MESSAGE`.
    match e {
        FrameError::TooLarge(_) => APP_ERR_MESSAGE_TOO_LARGE,
        FrameError::Io(_) | FrameError::Varint | FrameError::Decode(_) => APP_ERR_MALFORMED_MESSAGE,
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
