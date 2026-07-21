//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::num::NonZeroUsize;
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
    encode_message, is_unknown_variant, message::ProbeResponse, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::endpoint::{Accepting, Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::handlers::probe_rate_limit::{ProbeRateLimiter, ProbeRejectLayer};
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

/// Stake-lane probe-acceptance policy (#757, ADR 003 §Admission and
/// Priority). Reserves probe-hold headroom for stake-lane requesters —
/// registered operators issuing node-to-node cache-miss probes — so
/// end-client probe load cannot starve them under hold-budget pressure.
///
/// "Stake lane" is the `CapacityBond.isActive` predicate (registered + bond
/// ≥ minBond + not unbonding + not ejected), resolved from the same
/// chain-followed [`StakerSet`] the DHT handler consumes — no per-probe
/// chain read and no new chain wiring (the issue's constraint). It is a
/// faithful binary reading of ADR 003's `bondOf >= bond_required(...)`
/// prioritization; finer capacity-scaled bond tiering is out of scope.
///
/// Construction is gated by the operator: the runtime only builds this when
/// `cache.stake_lane_reserved_holds > 0`, so a single-lane deployment passes
/// `None` and the handler's hot path is unchanged.
#[derive(Clone)]
pub struct StakeLanePolicy {
    /// Chain-followed active-staker set, keyed by iroh `NodeId`. The probe
    /// requester's id (`conn.remote_id()`) is looked up directly — the
    /// `CapacityBond` holds the NodeId↔operator binding, so a `true` here
    /// means the requester is a registered, sufficiently-bonded operator.
    staker_set: Arc<dyn StakerSet>,
    /// Number of hold slots reserved for the stake lane. End-client probes
    /// are shed once `probe_hold_slots_used >= max_holds.saturating_sub(reserved_holds)`
    /// (the `saturating_sub` makes `reserved_holds >= max_holds` clamp the
    /// end-client ceiling to `0`, i.e. "stake lane only"). Typed
    /// [`NonZeroUsize`] so the "reservation off" case is *unrepresentable*
    /// here — it is encoded as `None` at the call site — rather than relying
    /// on a documented `> 0` convention.
    reserved_holds: NonZeroUsize,
    /// The configured `max_probe_holds` budget — the ceiling the reservation
    /// is subtracted from. Carried here (rather than re-read from the cache)
    /// because `max_probe_holds` is restart-required, so the value is stable
    /// for the handler's lifetime.
    max_holds: usize,
}

impl std::fmt::Debug for StakeLanePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StakeLanePolicy")
            .field("reserved_holds", &self.reserved_holds)
            .field("max_holds", &self.max_holds)
            .finish_non_exhaustive()
    }
}

impl StakeLanePolicy {
    /// Build a policy. `reserved_holds` is [`NonZeroUsize`] so the "off" case
    /// (a zero reservation) cannot be constructed — callers encode it as
    /// `None`. `max_holds` is the configured `max_probe_holds`.
    #[must_use]
    pub fn new(
        staker_set: Arc<dyn StakerSet>,
        reserved_holds: NonZeroUsize,
        max_holds: usize,
    ) -> Self {
        Self {
            staker_set,
            reserved_holds,
            max_holds,
        }
    }
}

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
    /// ADR 005 §Probe rate limiting three-layer token-bucket limiter
    /// (global → per-IP → per-peer, trusted-IP exempting per-IP only). Runs in
    /// addition to `limiter` (the connection-level [`ConnectionLimiter`]) — see
    /// [`crate::handlers::probe_rate_limit`] for why both layers run.
    probe_rate_limiter: Arc<ProbeRateLimiter>,
    /// Cache engine — queried for blob presence and the probe-triggered
    /// eviction hold (ADR 005 §Probe-triggered eviction hold).
    cache: CacheEngine,
    /// Operator Ethereum key used to produce the EIP-712 `slash_sig`
    /// (#406 loads it; ADR 014 §1 mandates it on every response).
    eth_signer: Arc<PrivateKeySigner>,
    /// `SlashJudge` EIP-712 domain, built once from
    /// `blockchain.{slash_judge_address,chain_id}`.
    slash_domain: Eip712Domain,
    /// Live per-MB delivery-rate bounds clamping `rate_per_mb` before signing
    /// (ADR 005 §Rate bounds validation). Seeded from the on-chain
    /// `getRateBounds()` at startup and updated in place by the
    /// `RateBoundsUpdated` watcher (#1172), so a governance retune takes effect
    /// without a restart.
    rate_bounds: crate::rate_bounds::RateBounds,
    /// ADR 015 master switch (`network.enable_0rtt`). When `true`, this
    /// handler overrides `on_accepting` to read the probe as pre-handshake
    /// 0-RTT. When `false`, the default `on_accepting` is used. The 1-RTT
    /// downgrade is effected client-side (`probe_once` emits no early data
    /// when off) — not by this handler refusing 0-RTT; see the
    /// `on_accepting` doc and ADR 015 §"Replay Safety Is Client-Side".
    enable_0rtt: bool,
    /// Optional stake-lane probe-acceptance reservation (#757). `None` (the
    /// single-lane default) makes the hold-admission path identical to
    /// pre-#757 behaviour; `Some` reserves hold headroom for registered
    /// node-to-node requesters under budget pressure.
    stake_lane: Option<StakeLanePolicy>,
}

impl std::fmt::Debug for ProbeHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeHandler")
            .field("node_id", &self.node_id)
            .field("rate_per_mb", &self.rate_per_mb)
            .field("rate_bounds", &self.rate_bounds)
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
        probe_rate_limiter: Arc<ProbeRateLimiter>,
        cache: CacheEngine,
        eth_signer: Arc<PrivateKeySigner>,
        slash_domain: Eip712Domain,
        rate_bounds: crate::rate_bounds::RateBounds,
        enable_0rtt: bool,
        stake_lane: Option<StakeLanePolicy>,
    ) -> Self {
        Self {
            node_id,
            rate_per_mb,
            metrics,
            limiter,
            probe_rate_limiter,
            cache,
            eth_signer,
            slash_domain,
            rate_bounds,
            enable_0rtt,
            stake_lane,
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

        // ADR 005 §Probe rate limiting: three-layer token-bucket limiter
        // (global → per-IP → per-peer) applied *before* any signature is
        // computed and *before* any eviction-hold slot is allocated. It runs in
        // addition to the `ConnectionLimiter` permit above (per-source IP +
        // global concurrency) — see `probe_rate_limit` module docs for why
        // both run. Both the requester NodeId and the source IP are available
        // pre-stream, so a rejected probe never opens a bidi stream.
        let requester = NodeId::from_bytes(*conn.remote_id().as_bytes());
        let peer_ip = crate::rate_limit::peer_ip(&conn);
        if let Err(layer) = self.probe_rate_limiter.check(&requester, peer_ip) {
            // Load-shedding, not a protocol fault: don't return `Err` (which
            // iroh would log per rejection, amplifying log volume under flood —
            // exactly what the attacker wants). The limiter already bumped the
            // per-layer rejection counter; emit a debug log too so operators
            // can correlate the offending peer/IP, matching the per-source
            // (`ConnectionLimiter::acquire_inner`) and DHT
            // (`close_stream_with_rate_limit`) reject paths.
            tracing::debug!(
                layer = layer.as_str(),
                requester = ?requester,
                peer_ip = ?peer_ip,
                "probe request rejected by the three-layer rate limiter"
            );
            conn.close(
                VarInt::from_u32(APP_ERR_RATE_LIMITED),
                layer.as_str().as_bytes(),
            );
            // Wait briefly for the close frame so the peer observes the 0x10
            // RATE_LIMITED code and the layer label — except on the global
            // layer, where a flood would otherwise park a task per rejection
            // (operators pivot on the metric counter, not the close reason,
            // for a global flood). Per-peer/per-IP rejections are bounded by
            // their own buckets, so the capped wait is safe.
            if layer != ProbeRejectLayer::Global {
                let _ = tokio::time::timeout(REJECTION_CLOSE_TIMEOUT, conn.closed()).await;
            }
            return Ok(());
        }

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

        // Stake-lane probe-acceptance reservation (#757, ADR 003 §Admission
        // and Priority). Under hold-budget pressure, reserve the last
        // `reserved_holds` slots for stake-lane requesters (registered
        // operators issuing node-to-node cache-miss probes) so end-client
        // load cannot starve them. Evaluated *before* `try_probe_hold` and
        // gated by `self.stake_lane`, so it is a strict no-op for
        // single-lane operators (the `None` default). Soft and best-effort:
        // the slots-used read races a concurrent hold, which is acceptable
        // for a priority heuristic (admission is implementation-defined per
        // ADR 003) and never a safety property.
        // Sample `slots_used` once here and reuse it for the slots gauge
        // below: on the shed path no hold is attempted, so the sampled value
        // stays current and a second lock+sweep is pure waste on exactly the
        // loaded path this gate fires under (#757 review).
        let mut slots_used = None;
        let stake_lane_reserved = match self.stake_lane.as_ref() {
            Some(policy) => {
                let used = self.cache.probe_hold_slots_used();
                slots_used = Some(used);
                // `is_stake_lane` is resolved lazily: `stake_lane_reserved_out`
                // calls it only after the cheap ceiling guards pass, so the
                // uncongested common path skips the staker-set lookup (an
                // RwLock read + NodeId conversion) entirely (#757 review).
                stake_lane_reserved_out(policy.reserved_holds.get(), policy.max_holds, used, || {
                    let requester = NodeId::from_bytes(*conn.remote_id().as_bytes());
                    policy.staker_set.is_active(&requester)
                })
            }
            None => false,
        };

        // Probe-triggered eviction hold (ADR 005 §Probe-triggered eviction
        // hold). Only `Held` permits signing `has_blob: true` — the blob is
        // present, not operator-evicted, and a 35s hold is guaranteed. The
        // outcome enum already classifies each `has_blob: false` cause
        // (absent/evicted, holds disabled, budget exhausted) so no second
        // cache lookup is needed. The hold is taken *after* the rate limiter
        // (ADR 005 §Probe rate limiting: hold admission occurs only after the
        // limiter passes — do not reorder).
        let (has_blob, total_bytes) = if stake_lane_reserved {
            // End-client probe shed to protect stake-lane headroom. Sign
            // `has_blob: false` (the blob may be present, but a
            // non-guaranteed hold must never risk a phantom slash — ADR
            // 005) and count the reservation distinctly from genuine
            // budget exhaustion / config disable (#757).
            self.metrics.probe_stake_lane_reserved();
            tracing::debug!(
                hash = %hash,
                "probe hold refused: stake-lane reservation (end-client under \
                 budget pressure); signing has_blob:false"
            );
            (false, None)
        } else {
            match self.cache.try_probe_hold(hash).await {
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
                    // Present but un-holdable because every hold slot is live: an
                    // availability degradation under genuine load, never a safety
                    // fault (ADR 005 §Hold budget). The actionable remedy is to
                    // raise `max_probe_holds`, so this is the counter that drives
                    // that alert (#739).
                    self.metrics.probe_hold_violation();
                    // Don't log `probe_hold_slots_used()` here: it re-acquires the
                    // `probe_holds` lock and sweeps, and on this hot refusal path
                    // the value is a foregone ~`max` anyway. The gauge is published
                    // once per probe below.
                    tracing::debug!(
                        hash = %hash,
                        "probe hold refused: budget exhausted; signing has_blob:false"
                    );
                    (false, None)
                }
                Ok(ProbeHoldOutcome::HoldsDisabled) => {
                    // Present but un-holdable because holds are disabled by config
                    // (`max_probe_holds == 0`). Counted separately from budget
                    // pressure (#739) so an intentional disable does not trip the
                    // "increase max_probe_holds" alert.
                    self.metrics.probe_holds_disabled();
                    tracing::debug!(
                        hash = %hash,
                        "probe hold refused: holds disabled (max_probe_holds=0); signing has_blob:false"
                    );
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
            }
        };
        // On the shed path no hold was attempted, so the value sampled for
        // the gate is still current — reuse it instead of re-acquiring the
        // lock and sweeping again. Off that path a hold may have been taken,
        // so re-sample for an accurate gauge (#757 review).
        self.metrics.probe_hold_slots(if stake_lane_reserved {
            slots_used.unwrap_or_else(|| self.cache.probe_hold_slots_used())
        } else {
            self.cache.probe_hold_slots_used()
        });

        // Clamp the quoted rate to the configured delivery bounds before
        // signing (ADR 005 §Rate bounds validation): clamp-and-warn keeps
        // the node operational across governance transitions.
        let raw_rate = self.rate_per_mb.load(Ordering::Relaxed);
        let rate_per_mb = self.rate_bounds.clamp(raw_rate);
        if rate_per_mb != raw_rate {
            self.metrics.rate_bounds_clamped();
            tracing::warn!(
                raw_rate,
                clamped = rate_per_mb,
                floor = self.rate_bounds.floor(),
                ceiling = self.rate_bounds.ceiling(),
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

/// Decide whether an end-client (non-stake-lane) probe should be refused a
/// probe hold to preserve reserved headroom for stake-lane requesters —
/// registered operators issuing node-to-node cache-miss probes (#757, ADR
/// 003 §Admission and Priority).
///
/// Returns `true` only when ALL of: a reservation is configured
/// (`reserved > 0`), holds are enabled (`max_holds > 0`), current hold usage
/// has reached the end-client ceiling (`slots_used >= max_holds - reserved`),
/// and the requester is **not** in the stake lane. The last `reserved` slots
/// are thereby kept available for the stake lane.
///
/// `is_stake_lane` is evaluated **lazily** and only after the three cheap
/// guards above have passed, so callers can pass a closure that performs the
/// staker-set lookup (an `RwLock` read + `NodeId` conversion) and have it
/// skipped entirely on the uncongested common path (#757 review).
///
/// `reserved == 0` is the default and makes this an unconditional no-op so
/// single-lane operators are unaffected. The `max_holds == 0` short-circuit
/// keeps an intentional holds-disabled config (which `try_probe_hold` maps
/// to [`ProbeHoldOutcome::HoldsDisabled`]) from being mis-attributed to a
/// reservation refusal.
///
/// This is a **soft, best-effort** gate: `slots_used` is sampled before the
/// atomic `try_probe_hold`, so a concurrent hold may still cross the
/// boundary. That is acceptable for a priority heuristic — it is never a
/// safety property (ADR 003 frames admission as implementation-defined).
fn stake_lane_reserved_out(
    reserved: usize,
    max_holds: usize,
    slots_used: usize,
    is_stake_lane: impl FnOnce() -> bool,
) -> bool {
    reserved > 0
        && max_holds > 0
        && slots_used >= max_holds.saturating_sub(reserved)
        && !is_stake_lane()
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
            // ADR 013: an unknown enum discriminant is a Tier-2
            // graceful-evolution signal — close with UNSUPPORTED_MESSAGE
            // (0x01), not MALFORMED_MESSAGE (0x03). A genuine postcard/varint
            // parse fault (in-range discriminant, bad payload) stays MALFORMED.
            let app_code = if is_unknown_variant::<ProbeMessage>(&frame) {
                APP_ERR_UNSUPPORTED_MESSAGE
            } else {
                APP_ERR_MALFORMED_MESSAGE
            };
            reset(send, recv, app_code);
            tracing::warn!(
                app_code,
                error = %e,
                "probe message decode failed"
            );
            Err(ProbeReadError {
                err: anyhow::anyhow!("probe decode failed: {e}"),
                app_code,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::stake_lane_reserved_out;

    /// With no reservation configured (`reserved == 0`) the gate is a
    /// no-op: even a non-stake requester at a full hold budget is never
    /// reserved out. This is the default-off invariant — single-lane
    /// operators must be entirely unaffected (#757).
    #[test]
    fn no_reservation_never_reserves_out() {
        assert!(!stake_lane_reserved_out(0, 256, 256, || false));
        assert!(!stake_lane_reserved_out(0, 256, 0, || false));
    }

    /// A stake-lane (registered-operator) requester is never reserved out,
    /// even with the budget fully consumed — the reservation exists to
    /// protect exactly these node-to-node cache-miss probes.
    #[test]
    fn stake_lane_requester_never_reserved_out() {
        assert!(!stake_lane_reserved_out(8, 256, 256, || true));
        assert!(!stake_lane_reserved_out(8, 256, 255, || true));
    }

    /// An end-client probe is admitted while hold usage is below the
    /// end-client ceiling (`max_holds - reserved`) and refused once usage
    /// reaches it, reserving the last `reserved` slots for the stake lane.
    #[test]
    fn end_client_refused_at_reserved_ceiling() {
        // reserved=8, max=256 => end-client ceiling is 248.
        assert!(!stake_lane_reserved_out(8, 256, 247, || false));
        assert!(stake_lane_reserved_out(8, 256, 248, || false));
        assert!(stake_lane_reserved_out(8, 256, 256, || false));
    }

    /// Holds disabled (`max_holds == 0`) short-circuits to `false` so the
    /// reservation gate never pre-empts the `HoldsDisabled` outcome — the
    /// `saturating_sub` would otherwise yield a `0` ceiling and refuse
    /// every end-client, mis-attributing an intentional disable.
    #[test]
    fn holds_disabled_is_not_a_reservation_refusal() {
        assert!(!stake_lane_reserved_out(8, 0, 0, || false));
    }

    /// Reserving the entire budget (`reserved >= max_holds`) yields a `0`
    /// ceiling: every end-client probe is reserved out whenever holds are
    /// enabled. An aggressive but valid "stake lane only" configuration.
    #[test]
    fn reserving_full_budget_excludes_all_end_clients() {
        assert!(stake_lane_reserved_out(256, 256, 0, || false));
        assert!(stake_lane_reserved_out(512, 256, 0, || false));
    }

    /// The `is_stake_lane` closure is consulted only after the cheap ceiling
    /// guards pass, so the staker-set lookup is skipped on the uncongested
    /// common path (#757 review). Each cheap guard failing must short-circuit
    /// before the closure runs.
    #[test]
    fn is_stake_lane_lookup_is_skipped_below_ceiling() {
        let consulted = std::cell::Cell::new(false);
        let probe = || {
            consulted.set(true);
            false
        };
        // reserved == 0 (default-off), holds disabled, and usage below the
        // end-client ceiling each short-circuit before the lookup.
        assert!(!stake_lane_reserved_out(0, 256, 256, probe));
        assert!(!stake_lane_reserved_out(8, 0, 0, probe));
        assert!(!stake_lane_reserved_out(8, 256, 247, probe));
        assert!(!consulted.get(), "staker-set lookup ran below the ceiling");

        // At the ceiling the lookup is required and must run.
        assert!(stake_lane_reserved_out(8, 256, 248, probe));
        assert!(consulted.get(), "staker-set lookup skipped at the ceiling");
    }
}
