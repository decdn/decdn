//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::B256;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, Hash, ProbeHoldOutcome};
use decdn_incentive::ProbeSlashData;
use decdn_protocol::{
    ALPN_PROBE, APP_ERR_RATE_LIMITED, FrameError, ProbeMessage, ProbeResponseBody, decode_message,
    encode_message, message::ProbeResponse, read_frame, write_frame,
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
//
// `APP_ERR_NO_ERROR = 0` is the implicit QUIC "no app error" default and is
// what the handler returns for transport-level conditions (read timeout,
// mid-frame EOF, peer reset) where no protocol fault occurred. Named to
// avoid magic-number drift; #577 M1 motivated splitting it out from
// `APP_ERR_MALFORMED_MESSAGE` so peers don't apply protocol-fault backoff
// to a transport drop.
const APP_ERR_NO_ERROR: u32 = 0x00;
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
    /// Cache engine — queried for blob presence and the probe-triggered
    /// eviction hold (ADR 005 §Probe-triggered eviction hold).
    cache: CacheEngine,
    /// Operator Ethereum key used to produce the EIP-712 `slash_sig`
    /// (#406 loads it; ADR 014 §1 mandates it on every response).
    eth_signer: Arc<PrivateKeySigner>,
    /// `SlashJudge` EIP-712 domain, built once from
    /// `blockchain.{slash_judge_address,chain_id}`.
    slash_domain: Eip712Domain,
    /// Lower/upper clamp bounds for `rate_per_mb` before signing (ADR 005
    /// §Rate bounds validation). Locally enforced stand-in for on-chain
    /// `getRateBounds()`.
    delivery_floor: u64,
    delivery_ceiling: u64,
    /// ADR 015 master switch (`network.enable_0rtt`). When `true`, this
    /// handler overrides `on_accepting` to read the probe as pre-handshake
    /// 0-RTT. When `false`, the default `on_accepting` is used. The 1-RTT
    /// downgrade is effected client-side (`probe_once` emits no early data
    /// when off) — not by this handler refusing 0-RTT; see the
    /// `on_accepting` doc and ADR 015 §"Replay Safety Is Client-Side".
    enable_0rtt: bool,
}

impl std::fmt::Debug for ProbeHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeHandler")
            .field("node_id", &self.node_id)
            .field("rate_per_mb", &self.rate_per_mb)
            .field("delivery_floor", &self.delivery_floor)
            .field("delivery_ceiling", &self.delivery_ceiling)
            .field("enable_0rtt", &self.enable_0rtt)
            .finish_non_exhaustive()
    }
}

impl ProbeHandler {
    pub const ALPN: &'static [u8] = ALPN_PROBE;

    #[allow(clippy::missing_const_for_fn)] // Arc::new isn't const.
    #[allow(clippy::too_many_arguments)] // wiring struct; each arg is distinct runtime state.
    pub fn new(
        node_id: PublicKey,
        rate_per_mb: Arc<AtomicU64>,
        metrics: Arc<Metrics>,
        limiter: Arc<ConnectionLimiter>,
        cache: CacheEngine,
        eth_signer: Arc<PrivateKeySigner>,
        slash_domain: Eip712Domain,
        delivery_floor: u64,
        delivery_ceiling: u64,
        enable_0rtt: bool,
    ) -> Self {
        Self {
            node_id,
            rate_per_mb,
            metrics,
            limiter,
            cache,
            eth_signer,
            slash_domain,
            delivery_floor,
            delivery_ceiling,
            enable_0rtt,
        }
    }

    // Linear, ordered protocol sequence (limit → accept → read → hold →
    // clamp → sign → write → close). Splitting it would scatter the ADR-013
    // app-error-code mapping and the ADR-005 ordering invariants across
    // helpers and make them harder to audit against the spec — same
    // rationale as the `read_probe_request` cognitive-complexity allow.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
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

        let hash = Hash::from_bytes(req.hash);

        // Probe-triggered eviction hold (ADR 005 §Probe-triggered eviction
        // hold). Only `Held` permits signing `has_blob: true` — the blob is
        // present, not operator-evicted, and a 35s hold is guaranteed. The
        // outcome enum already classifies the miss (absent vs budget
        // exhausted) so no second cache lookup is needed. The hold is taken
        // *after* the rate limiter (ADR 005 §Probe rate limiting: hold
        // admission occurs only after the limiter passes — do not reorder).
        let (has_blob, total_bytes) = match self.cache.try_probe_hold(hash).await {
            Ok(ProbeHoldOutcome::Held) => {
                let size = self
                    .cache
                    .inspect(hash)
                    .await
                    .ok()
                    .and_then(|p| p.size_bytes);
                (true, size)
            }
            Ok(ProbeHoldOutcome::BudgetExhausted) => {
                // Present but un-holdable: an availability degradation, never
                // a safety fault (ADR 005 §Hold budget). Answer false.
                self.metrics.probe_hold_violation();
                (false, None)
            }
            // Blob genuinely absent or operator-evicted — a true negative,
            // no signal needed.
            Ok(ProbeHoldOutcome::Unavailable) => (false, None),
            // A transient cache fault is *not* the same as "absent": the
            // node may actually hold the blob. We still conservatively
            // answer `has_blob: false` (never risk a phantom slash, ADR
            // 005), but a degrading backend must be operator-visible rather
            // than indistinguishable from a normal miss. The registry has
            // no metric for this; a warn log is the actionable signal.
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "cache error during probe hold check; answering has_blob:false"
                );
                (false, None)
            }
        };
        self.metrics
            .probe_hold_slots(self.cache.probe_hold_slots_used());

        // Clamp the quoted rate to the configured delivery bounds before
        // signing (ADR 005 §Rate bounds validation): clamp-and-warn keeps
        // the node operational across governance transitions.
        let raw_rate = self.rate_per_mb.load(Ordering::Relaxed);
        let rate_per_mb = raw_rate.clamp(self.delivery_floor, self.delivery_ceiling);
        if rate_per_mb != raw_rate {
            self.metrics.rate_bounds_clamped();
            tracing::warn!(
                raw_rate,
                clamped = rate_per_mb,
                floor = self.delivery_floor,
                ceiling = self.delivery_ceiling,
                "rate_per_mb clamped to delivery bounds before signing ProbeResponse"
            );
        }

        let body = ProbeResponseBody {
            hash: req.hash,
            has_blob,
            rate_per_mb,
            timestamp_us: req.timestamp_us,
        };
        // EIP-712 secp256k1 slash_sig over the frozen signed set
        // {hash, has_blob, rate_per_mb, timestamp_us} (ADR 014 §1). Mandatory
        // and non-empty on every response — a signing failure fails the
        // handler rather than emitting an unsigned response a requester MUST
        // reject anyway.
        let slash_data = ProbeSlashData {
            hash: B256::from(req.hash),
            has_blob,
            rate_per_mb,
            timestamp_us: req.timestamp_us,
        };
        let slash_sig = match slash_data.sign(self.eth_signer.as_ref(), &self.slash_domain) {
            Ok(sig) => sig.as_bytes().to_vec(),
            Err(e) => {
                // Operator-actionable infra fault (key locked, remote/HSM
                // signer offline). The bare iroh `AcceptError` carries no
                // context, so log it explicitly before failing the handler
                // (we must NOT emit an unsigned response — ADR 014 §1).
                tracing::error!(
                    error = %e,
                    "probe slash_sig signing failed (eth signer unavailable?); \
                     failing probe"
                );
                return Err(anyhow::anyhow!("probe slash_sig signing failed: {e}"));
            }
        };

        let resp = ProbeResponse {
            body,
            total_bytes,
            slash_sig,
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
    /// ADR 015. This override is a *latency/structuring* choice, **not**
    /// the replay-safety boundary. iroh sets `max_early_data_size =
    /// u32::MAX` on every server TLS config, so a handler that keeps the
    /// default `accepting.await` STILL has the client's 0-RTT accepted and
    /// still processes the early data (just post-handshake). Per-ALPN
    /// safety is enforced client-side: `probe_once` is the only code that
    /// emits early data and it is hard-wired to `ALPN_PROBE` (idempotent,
    /// replay-safe). See ADR 015 §"Replay Safety Is Client-Side" and the
    /// `default_on_accepting_still_accepts_0rtt_safety_is_client_side`
    /// characterization test.
    ///
    /// What the override buys: the probe is read as true 0-RTT *before*
    /// handshake completion instead of post-handshake. `into_0rtt()`
    /// accepts the client's early data when a resumption ticket is present
    /// and enables 0.5-RTT otherwise; a cold client is an ordinary 1-RTT
    /// connection. We resolve via `handshake_completed()` and serve
    /// through the unchanged `serve()` path so the limiter, metrics, and
    /// ADR 013 error mapping operate on a connection with a known,
    /// authenticated peer. A completed handshake also means the server
    /// has emitted its `NewSessionTicket` (rustls defaults
    /// `send_tls13_tickets` to a non-zero value — a dependency default,
    /// not a protocol guarantee), so the peer is counted toward the
    /// approximate session-ticket gauge.
    ///
    /// With the master switch off we keep the default `on_accepting`: the
    /// server no longer reads probes pre-handshake or feeds the gauge.
    /// That alone does not refuse 0-RTT (the TLS layer still would) — the
    /// genuine 1-RTT downgrade comes from `probe_once` not emitting early
    /// data when the switch is off.
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
        // #577 M1: transport-level fault (short read, peer reset,
        // mid-frame EOF) is NOT a protocol fault. Mirrors the
        // timeout-path convention in `read_probe_request` — peers must
        // not apply MALFORMED-class backoff/penalty for a dropped
        // connection.
        FrameError::Io(_) => APP_ERR_NO_ERROR,
        FrameError::Varint | FrameError::Decode(_) => APP_ERR_MALFORMED_MESSAGE,
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
            // ADR 013 defines no timeout-specific code; use the named
            // `APP_ERR_NO_ERROR` constant (same convention now applied
            // to `FrameError::Io(_)` per #577 M1).
            reset(send, recv, APP_ERR_NO_ERROR);
            tracing::warn!(
                timeout_ms = u64::try_from(PROBE_READ_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                "probe request read timed out"
            );
            return Err(ProbeReadError {
                err: anyhow::anyhow!("probe request timed out after {PROBE_READ_TIMEOUT:?}"),
                app_code: APP_ERR_NO_ERROR,
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
