//! `cdn/probe/v1` handler — unauthenticated latency + rate probe.

use std::num::NonZeroUsize;
use std::sync::Arc;
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
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::handlers::probe_rate_limit::{ProbeRateLimiter, ProbeRejectLayer};
use crate::metrics::{Metrics, ProbeHoldUnavailableReason};

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
/// `rate_per_mb` is the served price, fixed at startup. Reprice by restarting
/// the daemon (see `runtime::reload::warn_restart_required_sections`).
pub struct ProbeHandler {
    node_id: PublicKey,
    rate_per_mb: u64,
    metrics: Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    /// ADR 005 §Probe rate limiting three-layer token-bucket limiter
    /// (global → per-IP → per-peer). Runs in addition to `limiter` (the
    /// connection-level [`ConnectionLimiter`]) — see
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
            .finish_non_exhaustive()
    }
}

impl ProbeHandler {
    pub const ALPN: &'static [u8] = ALPN_PROBE;

    #[allow(clippy::missing_const_for_fn)] // Arc::new isn't const.
    #[allow(clippy::too_many_arguments)] // wiring struct; each arg is distinct runtime state.
    pub fn new(
        node_id: PublicKey,
        rate_per_mb: u64,
        metrics: Arc<Metrics>,
        limiter: Arc<ConnectionLimiter>,
        probe_rate_limiter: Arc<ProbeRateLimiter>,
        cache: CacheEngine,
        eth_signer: Arc<PrivateKeySigner>,
        slash_domain: Eip712Domain,
        rate_bounds: crate::rate_bounds::RateBounds,
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
            stake_lane,
        }
    }

    /// The advertised `total_bytes` for a blob already established as present.
    ///
    /// `total_bytes` is an unsigned, optional hint outside the `slash_sig`
    /// typehash, so `None` degrades cleanly to "unknown but fetchable" — no
    /// consumer treats it as a presence signal. An `Err` here is still worth a
    /// log: every caller has *just* proven the blob present, so a failing
    /// `inspect` means the store started degrading in between, which is a
    /// strictly stronger signal than the plain miss the `Err` arms warn about.
    async fn inspect_size(&self, hash: Hash) -> Option<u64> {
        match self.cache.inspect(hash).await {
            Ok(preview) => preview.size_bytes,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    %hash,
                    "inspect failed for a blob just proven present; advertising without a size"
                );
                None
            }
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
        // hold). The hold is **best-effort**: a node answers `has_blob: true`
        // whenever it holds the blob and has not refused it, then keeps a 35s
        // eviction hold *if it can* so the blob is still resident for the
        // follow-up pull. Budget pressure or a stake-lane reservation only means
        // "place no hold" — never "deny an honest answer", so a probe flood that
        // fills the hold cache can never suppress a truthful `has_blob`.
        //
        // What an unheld advertisement actually costs, if the blob is
        // LRU-evicted before the pull lands: one wasted round trip. The delivery
        // path answers a plain `NotFound` and the requester goes elsewhere. It
        // is NOT a slash (no offense pairs a probe with a miss — ADR 014), and
        // it is NOT a reputation penalty either: `classify_refusal` rates
        // `NotFound` as `RefusalVerdict::Transient`, which briefly suppresses
        // the (peer, hash) pair *without* scoring the peer, because a miss is
        // not attributable to it (see `node_origin.rs`). Do not justify this
        // path by reputation — nothing records an outcome for it.
        //
        // The hold is attempted *after* the rate limiter (ADR 005 §Probe rate
        // limiting: hold admission occurs only after the limiter passes — do not
        // reorder).
        let (has_blob, total_bytes) = if stake_lane_reserved {
            // End-client shed to protect stake-lane hold headroom (#757): place
            // no hold, but still answer honestly — presence does not depend on a
            // slot. `has()` already folds in `refuses()` (denied / chain-denied /
            // operator-evicted), so it alone settles presence; the second
            // `refuses` call is not a distinct predicate but a re-check that
            // narrows the TOCTOU window the `await` opens, because signing
            // `has_blob: true` for a blacklisted hash is a blacklist-violation
            // offense (ADR 014).
            self.metrics
                .probe_hold_unavailable(ProbeHoldUnavailableReason::StakeLaneReserved);
            match self.cache.has(hash).await {
                Ok(true) if !self.cache.refuses(hash) => (true, self.inspect_size(hash).await),
                Ok(_) => (false, None),
                // Same reasoning as the `try_probe_hold` `Err` arm below: a store
                // fault is not "absent" and must not be silently indistinguishable
                // from one. This path runs only under hold pressure — precisely
                // when a degrading backend is most worth surfacing.
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        %hash,
                        "cache error during stake-lane shed presence check; \
                         answering has_blob:false"
                    );
                    (false, None)
                }
            }
        } else {
            match self.cache.try_probe_hold(hash).await {
                Ok(ProbeHoldOutcome::Held) => (true, self.inspect_size(hash).await),
                Ok(ProbeHoldOutcome::BudgetExhausted) => {
                    // Present but every hold slot is live: advertise anyway and
                    // forgo the hold. A probe flood that fills the best-effort
                    // hold cache cannot suppress an honest answer — the blob is
                    // here now (a `BudgetExhausted` outcome already means present
                    // and not refused). If it is evicted before the pull, that
                    // costs one wasted round trip (see the block comment above).
                    // Raising `max_probe_holds` is still the remedy for the lost
                    // holds (#739).
                    self.metrics
                        .probe_hold_unavailable(ProbeHoldUnavailableReason::Exhausted);
                    (true, self.inspect_size(hash).await)
                }
                Ok(ProbeHoldOutcome::HoldsDisabled) => {
                    // Holds disabled by config (`max_probe_holds == 0`) is the
                    // operator's explicit opt-out from advertising *store-backed*
                    // content: answer `has_blob: false` so this node is not
                    // selected off the evictable store. Distinct from budget
                    // pressure (which still advertises) — a deliberate choice,
                    // not load (#739).
                    //
                    // It is not a blanket "answer false to everything": the
                    // origin-held fallback below takes no hold and so is
                    // unaffected by this setting. A node with a configured origin
                    // still advertises origin-servable content with
                    // `max_probe_holds == 0`.
                    self.metrics
                        .probe_hold_unavailable(ProbeHoldUnavailableReason::Disabled);
                    (false, None)
                }
                // Blob genuinely absent, operator-evicted, or refused
                // (blacklisted/denied) — a true negative, no signal needed.
                Ok(ProbeHoldOutcome::Unavailable) => (false, None),
                // A transient cache fault is *not* the same as "absent": the
                // node may actually hold the blob. We still conservatively
                // answer `has_blob: false` (a store it cannot read is one it
                // cannot serve), but a degrading backend must be
                // operator-visible rather than indistinguishable from a normal
                // miss. The registry has no metric for this; a warn log is the
                // actionable signal.
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "cache error during probe hold check; answering has_blob:false"
                    );
                    (false, None)
                }
            }
        };

        // Origin-held fallback (#1130). If the store can't back a
        // `has_blob: true` — the blob was never pulled into it, holds are
        // disabled, or the store read faulted — but it is servable from a
        // configured origin (fs directory entry, or a present pin), advertise it
        // anyway so cold origin content is discoverable on the first probe
        // rather than only after a warm.
        //
        // Origin content takes **no** eviction hold: it lives on disk / behind
        // the origin, not in the evictable store, so hold-budget pressure and
        // `max_probe_holds` are both irrelevant to it — which is why
        // `max_probe_holds == 0` silences store-backed advertisements but not
        // these. If the origin object later vanishes before the pull, the
        // delivery path answers a plain miss and the requester retries
        // elsewhere: one wasted round trip, not a slash (ADR 014) and not a
        // reputation penalty (see the hold block comment above).
        let (has_blob, total_bytes) =
            fold_origin_held((has_blob, total_bytes), self.cache.origin_held_size(hash));
        // Live-origin HEAD fallback (#1130 pt3). The index above covers fs
        // enumeration ∪ pins; http/s3 do not list, so a non-pinned bucket object
        // is invisible to it. When nothing has answered `true` yet, consult the
        // origin directly (`HEAD`/`HeadObject`, no body) with a TTL memo so a
        // non-pinned remote object is discoverable on the first probe rather than
        // never. Skipped once `has_blob` is already true — no point paying a HEAD
        // for a blob the store or the index already backs.
        let (has_blob, total_bytes) = if has_blob {
            (has_blob, total_bytes)
        } else {
            fold_origin_held(
                (has_blob, total_bytes),
                self.cache.origin_probe_size(hash).await,
            )
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

        // Raise the quoted rate to the governance delivery floor before signing
        // (ADR 005 §Rate bounds validation): clamp-and-warn keeps the node
        // operational across governance transitions.
        let raw_rate = self.rate_per_mb;
        let (rate_per_mb, floor) = self.rate_bounds.raise_to_floor(raw_rate);
        if rate_per_mb != raw_rate {
            self.metrics.rate_bounds_clamped();
            tracing::warn!(
                raw_rate,
                clamped = rate_per_mb,
                floor,
                "rate_per_mb raised to the delivery floor before signing ProbeResponse"
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

/// Fold the origin-held index into the store's `has_blob` decision (#1130).
///
/// If the store already backs `has_blob: true`, keep it. Otherwise, when the
/// blob is servable from a configured origin (`origin_size` is `Some`),
/// advertise it with the origin's size — cold origin content is discoverable
/// on the first probe. Pure so the policy is unit-testable without a live
/// connection.
const fn fold_origin_held(
    store_decision: (bool, Option<u64>),
    origin_size: Option<u64>,
) -> (bool, Option<u64>) {
    match store_decision {
        (true, bytes) => (true, bytes),
        (false, _) => match origin_size {
            Some(size) => (true, Some(size)),
            None => (false, None),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{fold_origin_held, stake_lane_reserved_out};

    /// A store-backed `has_blob: true` is preserved untouched, even if the
    /// origin also holds the blob — the store size wins (it's already held).
    #[test]
    fn fold_origin_held_keeps_store_hit() {
        assert_eq!(
            fold_origin_held((true, Some(100)), Some(200)),
            (true, Some(100))
        );
        assert_eq!(fold_origin_held((true, None), Some(200)), (true, None));
    }

    /// A store miss + origin hold → advertise with the origin's size (#1130).
    #[test]
    fn fold_origin_held_promotes_origin_content() {
        assert_eq!(
            fold_origin_held((false, None), Some(4096)),
            (true, Some(4096))
        );
    }

    /// A store miss with no origin hold stays a true negative.
    #[test]
    fn fold_origin_held_absent_stays_false() {
        assert_eq!(fold_origin_held((false, None), None), (false, None));
    }

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
