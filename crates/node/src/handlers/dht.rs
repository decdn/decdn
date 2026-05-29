//! `cdn/dht/v1` handler (ADR 022) — Kademlia content discovery.
//!
//! Request handling:
//!
//! - `FindNode` honored against the in-memory routing table.
//! - `Store` admission runs the full ADR 022 pipeline: holder ==
//!   authenticated `NodeId`, active-staker filter, per-publisher quota
//!   (200), global LRU (100k), per-hash provider cap (50), receiver-
//!   anchored TTL (1 h).
//! - `FindValue` consults the record store + the routing table; per
//!   ADR 022 §Lookup integrity the requester-side filters (XOR-distance,
//!   active-staker, negative-probe-cache, randomisation) are the
//!   requester's job and live in the iterative-lookup module
//!   ([`crate::dht::lookup`]).
//! - `BatchStore` admission (ADR 022 §Batch token accounting, #648):
//!   one batch-level `holder == authenticated NodeId` check (mismatch
//!   closes the stream with `MALFORMED_MESSAGE`), two-stage rate-limit
//!   accounting (stage 1 charged the inbound frame in `serve`; stage 2
//!   charges up to `n-1` more, partially admitting the batch under
//!   budget), then per-hash insert sharing one receiver-anchored
//!   `receive_us`. The reply is a `BatchStoreAck` with one bool per
//!   request hash, in request order. An oversize batch
//!   (> `MAX_BATCH_STORE_HASHES`) is rejected at wire decode with
//!   `MALFORMED_MESSAGE`.
//! - Response variants arriving on a server-accepted stream are
//!   rejected with `APP_ERR_UNSUPPORTED_MESSAGE`.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use decdn_protocol::{
    ALPN_DHT, APP_ERR_RATE_LIMITED, FrameError, MAX_CLOSER_NODES, decode_message, dht as wire,
    encode_message, read_frame, write_frame,
};
use iroh::PublicKey;
use iroh::TransportAddr;
use iroh::Watcher as _;
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::dht::{DhtRateLimiter, DhtRejectLayer, RecordStore, RoutingTable, StakerSet};
use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;

const ACCEPT_BI_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
const REJECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

// ADR 013 application error codes (also defined in `handlers::probe`; the
// codes are protocol-wide constants, not per-ALPN values).
const APP_ERR_UNSUPPORTED_MESSAGE: u32 = 0x01;
const APP_ERR_MESSAGE_TOO_LARGE: u32 = 0x02;
const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

/// Serves `cdn/dht/v1`. Owns the routing table + the per-request rate
/// limiter; `accept()` is reentrant — many concurrent streams from
/// different peers all share the same `Arc<DhtHandler>`.
///
/// Routing-table updates take a short `Mutex` — the table is touched once
/// per request to refresh the requester's recency and once again to
/// produce the `closer_nodes` snapshot. We use `std::sync::Mutex` rather
/// than `tokio::sync::Mutex` since the critical section is microseconds
/// long (no awaits) and the lock would just add scheduler overhead.
pub struct DhtHandler {
    self_id: PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    /// Receiver-side record store (ADR 022 §Content Records and TTL).
    /// `std::sync::Mutex` for the same reason as `routing`: the
    /// critical section is microseconds long (one `HashMap` mutation +
    /// two index updates) with no awaits.
    records: Arc<Mutex<RecordStore>>,
    /// Cached active-staker set consulted on every `Store` admission
    /// (ADR 022 §STORE Flow line 140). `Arc<dyn StakerSet>` so the
    /// runtime can swap a chain-backed implementation in once the
    /// on-chain origin-directory follow-up lands.
    staker_set: Arc<dyn StakerSet>,
    rate_limiter: Arc<DhtRateLimiter>,
    dispatch_limiter: Arc<ConnectionLimiter>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for DhtHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DhtHandler")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

impl DhtHandler {
    pub const ALPN: &'static [u8] = ALPN_DHT;

    /// Construct a handler. The routing table is empty at startup; PR 4
    /// of #320 wires bootstrap from the on-chain staker set, and the
    /// handler additionally updates the table from every incoming
    /// request's authenticated `NodeId` once it knows the peer is
    /// rate-limit admitted.
    #[must_use]
    pub fn new(
        self_id: PublicKey,
        rate_limiter: Arc<DhtRateLimiter>,
        dispatch_limiter: Arc<ConnectionLimiter>,
        metrics: Arc<Metrics>,
        staker_set: Arc<dyn StakerSet>,
        records: Arc<Mutex<RecordStore>>,
    ) -> Self {
        let routing = Arc::new(Mutex::new(RoutingTable::new(*self_id.as_bytes())));
        Self {
            self_id,
            routing,
            records,
            staker_set,
            rate_limiter,
            dispatch_limiter,
            metrics,
        }
    }

    /// Construct with a pre-built routing table. Used by tests and by the
    /// bootstrap path that wants to seed the table before the handler
    /// goes live.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // wiring struct; each arg is distinct runtime state.
    pub const fn with_routing(
        self_id: PublicKey,
        routing: Arc<Mutex<RoutingTable>>,
        rate_limiter: Arc<DhtRateLimiter>,
        dispatch_limiter: Arc<ConnectionLimiter>,
        metrics: Arc<Metrics>,
        staker_set: Arc<dyn StakerSet>,
        records: Arc<Mutex<RecordStore>>,
    ) -> Self {
        Self {
            self_id,
            routing,
            records,
            staker_set,
            rate_limiter,
            dispatch_limiter,
            metrics,
        }
    }

    /// Shared handle to the routing table — exposed so PR 4 of #320 can
    /// drive bucket refresh and bootstrap without re-allocating the
    /// `Arc<Mutex<RoutingTable>>` from outside.
    #[must_use]
    pub fn routing_table(&self) -> Arc<Mutex<RoutingTable>> {
        Arc::clone(&self.routing)
    }

    // Linear protocol sequence — same rationale as `ProbeHandler::serve`
    // for keeping the ADR 013 error-code mapping and the ADR 022 admission
    // order in one auditable function.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        // Layer 1: dispatch limiter (connection-level cap).
        let _permit = match self.dispatch_limiter.acquire(&conn) {
            Ok(p) => p,
            Err(reason) => {
                conn.close(
                    VarInt::from_u32(APP_ERR_RATE_LIMITED),
                    reason.as_str().as_bytes(),
                );
                if reason != RejectReason::GlobalFull {
                    let _ = tokio::time::timeout(REJECTION_CLOSE_TIMEOUT, conn.closed()).await;
                }
                return Ok(());
            }
        };
        let _guard = self.metrics.connection_guard();

        // Resolve the peer's NodeId once (it's the authenticated QUIC
        // identity and cannot change for the lifetime of the connection).
        // The IP, by contrast, is re-evaluated per stream below — iroh
        // can switch the selected path mid-connection (e.g. relay → direct
        // UDP after hole-punching) and caching the initial value would
        // permanently disable the per-IP rate-limit layer for any
        // connection that started relay-only.
        let peer_node_id = *conn.remote_id().as_bytes();

        // ADR 022 §DHT Rate Limiting check fires before any deserialization
        // of the per-stream frame body — we do it per stream below since
        // each DHT request is one stream. The check on the *connection*
        // here would be redundant; we run it inside the per-stream loop
        // because a single connection may carry multiple requests.

        // Iroh-side connection cap: DHT is short-lived request/response,
        // but a peer may bundle multiple requests on one connection. Accept
        // streams in a loop until the peer closes (or we time out waiting).
        loop {
            // Peer closed the connection (`Ok(Err)`) or we hit the idle
            // accept timeout (`Err`) — both mean we're done serving this
            // connection; break the per-stream loop.
            let Ok(Ok((send, recv))) =
                tokio::time::timeout(ACCEPT_BI_TIMEOUT, conn.accept_bi()).await
            else {
                break;
            };
            // Rate-limit per request (ADR 022 §DHT Rate Limiting). Re-read
            // `peer_ip` here so a relay→direct path switch over the
            // connection's lifetime engages the per-IP layer for
            // subsequent streams.
            let peer_ip = peer_ip(&conn);
            if let Err(layer) = self.rate_limiter.check(&peer_node_id, peer_ip) {
                Self::close_stream_with_rate_limit(send, recv, layer);
                continue;
            }
            // Refresh routing-table entry — the requester just authenticated
            // their NodeId via QUIC and successfully spent a rate-limit
            // token. That's the same admission criteria ADR 022 §Routing
            // Table demands for table insertion.
            self.note_peer_seen(peer_node_id);
            if let Err(e) = self.handle_one(peer_node_id, peer_ip, send, recv).await {
                // The per-request error already carries the ADR 013 app
                // error code via the stream reset inside `handle_one` /
                // `read_dht_request`; here we surface the failure to
                // operators via a debug log AND a counter bump so a node
                // running at default `RUST_LOG=info` still reflects "DHT
                // requests are failing" in scrapes without needing log
                // verbosity changes.
                self.metrics.dht_request_failed();
                tracing::debug!(error = %e, "dht request handling failed");
                // Drop this stream and continue accepting; the peer may
                // succeed on the next request.
            }
        }
        // Best-effort flush of any in-flight stream-finish bytes.
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, conn.closed()).await;
        Ok(())
    }

    // No `self` needed — kept as an inherent fn so the rate-limit close
    // path lives next to the only call site (the per-stream loop in
    // `serve`).
    fn close_stream_with_rate_limit(
        mut send: SendStream,
        mut recv: RecvStream,
        layer: DhtRejectLayer,
    ) {
        let code = VarInt::from_u32(APP_ERR_RATE_LIMITED);
        let _ = send.reset(code);
        let _ = recv.stop(code);
        tracing::debug!(
            layer = layer.as_str(),
            "dht request rejected by rate limiter",
        );
    }

    /// Refresh `peer_id` in the routing table. Silently ignores the
    /// node's own id (the table itself enforces that, but skipping the
    /// lock-acquire is cheap).
    fn note_peer_seen(&self, peer_id: [u8; 32]) {
        if peer_id == *self.self_id.as_bytes() {
            return;
        }
        if let Ok(mut table) = self.routing.lock() {
            table.insert(peer_id);
        }
    }

    /// Read one framed `DhtMessage`, dispatch on variant, write the
    /// response frame back. Each stream carries exactly one
    /// request/response pair. The authenticated `peer_node_id` is
    /// threaded in so `Store` / `BatchStore` admission can compare the
    /// wire-level `holder` against the QUIC-bound identity; `peer_ip` is
    /// threaded in for the `BatchStore` stage-2 rate-limit accounting
    /// (the per-IP layer keys on it).
    async fn handle_one(
        &self,
        peer_node_id: [u8; 32],
        peer_ip: Option<IpAddr>,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> anyhow::Result<()> {
        let req = match read_dht_request(&mut send, &mut recv).await {
            Ok(req) => req,
            Err(DhtReadError { err, app_code }) => {
                let _ = send.reset(VarInt::from_u32(app_code));
                let _ = recv.stop(VarInt::from_u32(app_code));
                return Err(err);
            }
        };

        // Most variants produce a response frame. `BatchStore` may
        // instead reject the whole batch with a stream-close-without-ack
        // (holder mismatch → `MALFORMED_MESSAGE`, ADR 022 §Batch token
        // accounting step 2) — `dispatch` signals that with `Err(code)`.
        let resp = match self.dispatch(peer_node_id, peer_ip, req) {
            Ok(resp) => resp,
            Err(app_code) => {
                let _ = send.reset(VarInt::from_u32(app_code));
                let _ = recv.stop(VarInt::from_u32(app_code));
                return Err(anyhow::anyhow!(
                    "dht request rejected with app error code {app_code:#x}"
                ));
            }
        };
        let payload = encode_message(&resp)
            .map_err(|e| anyhow::anyhow!("dht response encode failed: {e}"))?;
        write_frame(&mut send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("dht response write failed: {e}"))?;
        send.finish()
            .map_err(|e| anyhow::anyhow!("dht stream finish failed: {e}"))?;
        Ok(())
    }

    /// Convert a decoded request into a response. The argument's enum
    /// shape encodes the precondition that this is one of the request
    /// variants the handler implements — `read_dht_request` filters out
    /// responses-on-server-stream before producing a
    /// [`DhtServerRequest`], so this match is exhaustive over the request
    /// set without needing a panic or echo fallback.
    ///
    /// Returns `Err(app_code)` when the variant must be answered with a
    /// stream-close-without-ack rather than a response frame — the only
    /// such case today is a `BatchStore` whose batch-level `holder` does
    /// not match the authenticated `NodeId` (ADR 022 §Batch token
    /// accounting step 2: reject the whole batch with `MALFORMED_MESSAGE`).
    //
    // `msg` is moved-in so the `BatchStore` arm can take ownership of
    // `BatchStoreRequest::hashes` without re-allocating. The `Copy`
    // request variants are a no-op at the ABI level under the by-value
    // signature.
    #[allow(clippy::needless_pass_by_value)]
    fn dispatch(
        &self,
        peer_node_id: [u8; 32],
        peer_ip: Option<IpAddr>,
        msg: DhtServerRequest,
    ) -> Result<wire::DhtMessage, u32> {
        Ok(match msg {
            DhtServerRequest::FindNode(req) => {
                wire::DhtMessage::FindNodeResponse(self.handle_find_node(req))
            }
            DhtServerRequest::FindValue(req) => {
                wire::DhtMessage::FindValueResponse(self.handle_find_value(req))
            }
            DhtServerRequest::Store(req) => {
                wire::DhtMessage::StoreAck(self.handle_store(peer_node_id, req))
            }
            DhtServerRequest::BatchStore(req) => wire::DhtMessage::BatchStoreAck(
                self.handle_batch_store(peer_node_id, peer_ip, req)?,
            ),
        })
    }

    /// `BatchStore` admission (ADR 022 §STORE Flow Batched STORE + §Batch
    /// token accounting, #648). A single batch-level decision followed by
    /// per-hash admission identical to `n` separate `Store`s:
    ///
    /// 1. **Holder check (once).** `holder == authenticated NodeId`. On
    ///    mismatch the whole batch is rejected with `MALFORMED_MESSAGE`
    ///    (`Err(0x03)`) — a stream-close-without-ack — because a batch
    ///    claiming an identity the connection can't prove is malformed
    ///    (this is stricter than the per-hash `Store` path, which acks
    ///    `accepted: false`; see ADR 022 §Batch token accounting step 2).
    /// 2. **Stage-2 rate limit.** Stage 1 already charged the inbound
    ///    frame in [`Self::serve`]; here we charge up to `n - 1` more
    ///    tokens via [`DhtRateLimiter::admit_batch_extra`]. The first
    ///    `k = 1 + granted` hashes proceed; the `n - k` tail is acked
    ///    `false` without per-hash processing.
    /// 3. **Per-hash admission.** For each of the first `k` hashes: the
    ///    active-staker filter (batch-level holder, so the result is the
    ///    same for all hashes) then a receiver-anchored
    ///    [`RecordStore::insert_at`] sharing one `receive_us` for the
    ///    whole batch (ADR 022 §Content Records and TTL — a batch must
    ///    not have internally inconsistent TTLs).
    //
    // Linear admission sequence; same cognitive-complexity rationale as
    // `handle_store`.
    #[allow(clippy::cognitive_complexity)]
    fn handle_batch_store(
        &self,
        peer_node_id: [u8; 32],
        peer_ip: Option<IpAddr>,
        req: wire::BatchStoreRequest,
    ) -> Result<wire::BatchStoreAck, u32> {
        // Step 1: batch-level holder check. AC 19's size cap (≤ 256) is
        // already enforced at wire decode (`deserialize_batch_hashes` →
        // `MALFORMED_MESSAGE`), so by the time we're here `hashes.len()`
        // is in range.
        if req.holder != peer_node_id {
            self.metrics.dht_store_rejected_holder_mismatch();
            tracing::debug!(
                holder = ?req.holder,
                peer = ?peer_node_id,
                "dht BatchStore rejected: holder != authenticated NodeId"
            );
            return Err(APP_ERR_MALFORMED_MESSAGE);
        }
        self.metrics.dht_batch_store_received();

        let n = req.hashes.len();
        // Step 2: stage-2 token accounting. `k` hashes get per-hash
        // processing; the rest are deferred (`false`) by the rate limit.
        let extra = n.saturating_sub(1);
        let granted = self
            .rate_limiter
            .admit_batch_extra(&peer_node_id, peer_ip, extra);
        let k = granted.saturating_add(1).min(n);
        let deferred = n.saturating_sub(k);
        if deferred > 0 {
            self.metrics.dht_batch_store_hashes_deferred_rate_limit(
                u64::try_from(deferred).unwrap_or(u64::MAX),
            );
        }

        // Step 3: per-hash admission. The active-staker filter keys on
        // the batch-level holder, so its verdict is identical for every
        // hash — evaluate once. `receive_us` is anchored to a single
        // arrival timestamp for the whole batch.
        let active = self.staker_set.is_active(&req.holder);
        if !active {
            // Mirrors `handle_store` step 2's counter, once per rejected
            // hash so the per-hash dashboards see the same volume a
            // non-staked publisher's `n` separate `Store`s would produce.
            for _ in 0..k {
                self.metrics.dht_store_rejected_non_staked();
            }
            tracing::debug!(
                holder = ?req.holder,
                "dht BatchStore: holder not in active-staker set; all admitted hashes rejected"
            );
        }
        let now_us = now_us();
        let mut results = Vec::with_capacity(n);
        for (i, hash) in req.hashes.into_iter().enumerate() {
            // Tail beyond the rate-limit budget: deferred, no processing.
            if i >= k {
                results.push(false);
                continue;
            }
            if !active {
                results.push(false);
                continue;
            }
            let accepted = if let Ok(mut store) = self.records.lock() {
                let outcome = store.insert_at(req.holder, hash, now_us);
                if outcome.accepted() {
                    self.metrics.dht_store_accepted();
                    true
                } else {
                    self.metrics.dht_store_rejected_quota();
                    false
                }
            } else {
                tracing::error!(
                    "dht BatchStore: record-store mutex poisoned; rejecting remaining hashes"
                );
                false
            };
            results.push(accepted);
        }
        Ok(wire::BatchStoreAck { results })
    }

    /// `Store` admission (ADR 022 §STORE Flow):
    /// 1. `holder == authenticated NodeId` (caller invariant; rejects with
    ///    `accepted: false` on mismatch, never inserts).
    /// 2. `StakerSet::is_active(holder)` (caller's cached set).
    /// 3. Record-store insert under per-publisher quota + global LRU +
    ///    per-hash provider cap. The store returns whether the record
    ///    was newly inserted, refreshed, or hard-rejected at the
    ///    publisher cap — all of which map to `accepted` (refresh +
    ///    new) or `not accepted` (cap) on the wire.
    // Linear admission sequence (auth check → staker filter → insert →
    // counter bump per outcome). Splitting would scatter the ADR 022
    // §STORE Flow rule order across helpers; same rationale as the
    // `ProbeHandler::serve` cognitive-complexity allow upstream.
    #[allow(clippy::cognitive_complexity)]
    fn handle_store(&self, peer_node_id: [u8; 32], req: wire::StoreRequest) -> wire::StoreAck {
        // Step 1: holder must equal the QUIC-bound peer NodeId.
        // ADR 022 §STORE Flow line 140: "The receiving node MUST reject
        // any record whose `holder` does not equal the authenticated
        // NodeId of the inbound QUIC connection." Bump a dedicated
        // counter so the lying-`holder` attack rate is visible on the
        // scrape without `RUST_LOG=debug`.
        if req.holder != peer_node_id {
            self.metrics.dht_store_rejected_holder_mismatch();
            tracing::debug!(
                holder = ?req.holder,
                peer = ?peer_node_id,
                "dht Store rejected: holder != authenticated NodeId"
            );
            return wire::StoreAck {
                hash: req.hash,
                accepted: false,
            };
        }

        // Step 2: active-staker filter. A sustained non-zero rate on
        // this counter without matching `dht_store_accepted` growth
        // signals a Sybil-attempt — an attacker rotating fresh NodeIds
        // to spam records.
        if !self.staker_set.is_active(&req.holder) {
            self.metrics.dht_store_rejected_non_staked();
            tracing::debug!(
                holder = ?req.holder,
                "dht Store rejected: holder not in active-staker set"
            );
            return wire::StoreAck {
                hash: req.hash,
                accepted: false,
            };
        }

        // Step 3: receiver-anchored insert with all the cap rules.
        let now_us = now_us();
        let outcome = if let Ok(mut store) = self.records.lock() {
            Some(store.insert_at(req.holder, req.hash, now_us))
        } else {
            // Poisoned record-store mutex: respond `accepted: false`
            // so the publisher backs off rather than retrying into a
            // node whose admission path is broken.
            tracing::error!("dht Store: record-store mutex poisoned; rejecting publish");
            None
        };
        let accepted = match outcome {
            Some(o) if o.accepted() => {
                self.metrics.dht_store_accepted();
                true
            }
            Some(crate::dht::InsertOutcome::RejectedQuotaExceeded) => {
                self.metrics.dht_store_rejected_quota();
                false
            }
            // None = poisoned mutex; already logged above. No counter
            // for this path — it should fire 0 times in a healthy node
            // and the error log is the operator-actionable signal.
            _ => false,
        };
        wire::StoreAck {
            hash: req.hash,
            accepted,
        }
    }

    /// `FindValue` flow (ADR 022 §FIND\_VALUE Flow, responder side):
    /// the responder returns its known holders for `hash` plus the
    /// K-closest peers from the routing table. Per ADR 022 §Lookup
    /// integrity, requester-side filtering (XOR-distance check, active-
    /// staker filter, negative-probe-cache consultation, randomisation)
    /// is the requester's job; we don't apply it here.
    fn handle_find_value(&self, req: wire::FindValueRequest) -> wire::FindValueResponse {
        let now_us = now_us();
        let providers = if let Ok(mut store) = self.records.lock() {
            store.providers_at(&req.hash, now_us)
        } else {
            tracing::error!("dht FindValue: record-store mutex poisoned; returning empty");
            Vec::new()
        };
        wire::FindValueResponse {
            hash: req.hash,
            providers,
            closer_nodes: self.closest_to(&req.hash),
        }
    }

    fn handle_find_node(&self, req: wire::FindNodeRequest) -> wire::FindNodeResponse {
        // NB: do NOT insert `req.requester` here. It is an attacker-
        // controlled field on the wire — letting it through would allow
        // an authenticated peer to inject arbitrary (potentially
        // unreachable) NodeIds into the routing table without owning the
        // matching keys (routing-table poisoning). The authenticated peer
        // NodeId from the QUIC handshake is already refreshed once per
        // admitted request in `serve()`, which is the only honest signal
        // we have about who is on the other end.
        let closer_nodes = self.closest_to(&req.target);
        wire::FindNodeResponse {
            target: req.target,
            closer_nodes,
        }
    }

    fn closest_to(&self, target: &[u8; 32]) -> Vec<[u8; 32]> {
        let Ok(table) = self.routing.lock() else {
            // Poisoned lock — return empty; the caller will treat this as
            // "responder has nothing closer", which is honest at this
            // moment regardless of the lock state.
            return Vec::new();
        };
        table.closest(target, MAX_CLOSER_NODES)
    }
}

impl ProtocolHandler for DhtHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.serve(connection)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}

const fn frame_err_code(e: &FrameError) -> u32 {
    match e {
        FrameError::TooLarge(_) => APP_ERR_MESSAGE_TOO_LARGE,
        FrameError::Io(_) | FrameError::Varint | FrameError::Decode(_) => APP_ERR_MALFORMED_MESSAGE,
    }
}

struct DhtReadError {
    err: anyhow::Error,
    app_code: u32,
}

/// Request variants the DHT handler accepts on a server stream.
///
/// The enum is the narrowed shape produced by [`read_dht_request`] — it
/// excludes the response variants (responses belong on the client side of
/// the stream). Carrying the narrowing in a type, rather than a runtime
/// match-all fallback inside `dispatch`, keeps the dispatcher exhaustive
/// over the request set and prevents an accidentally-silent "I echoed a
/// bogus `FindNodeResponse`" failure mode if the wire enum grows new
/// response variants.
enum DhtServerRequest {
    FindNode(wire::FindNodeRequest),
    FindValue(wire::FindValueRequest),
    Store(wire::StoreRequest),
    BatchStore(wire::BatchStoreRequest),
}

async fn read_dht_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
) -> Result<DhtServerRequest, DhtReadError> {
    let reset = |send: &mut SendStream, recv: &mut RecvStream, code: u32| {
        let v = VarInt::from_u32(code);
        let _ = send.reset(v);
        let _ = recv.stop(v);
    };

    let frame = match tokio::time::timeout(READ_TIMEOUT, read_frame(recv)).await {
        Err(_) => {
            reset(send, recv, 0);
            return Err(DhtReadError {
                err: anyhow::anyhow!("dht request read timed out after {READ_TIMEOUT:?}"),
                app_code: 0,
            });
        }
        Ok(Err(e)) => {
            let app_code = frame_err_code(&e);
            reset(send, recv, app_code);
            return Err(DhtReadError {
                err: anyhow::anyhow!("dht frame read failed: {e}"),
                app_code,
            });
        }
        Ok(Ok(frame)) => frame,
    };

    match decode_message::<wire::DhtMessage>(&frame) {
        Err(e) => {
            reset(send, recv, APP_ERR_MALFORMED_MESSAGE);
            Err(DhtReadError {
                err: anyhow::anyhow!("dht decode failed: {e}"),
                app_code: APP_ERR_MALFORMED_MESSAGE,
            })
        }
        Ok((msg, _tail)) => {
            // Narrow the full wire enum down to the handler-supported
            // request set. Response variants on a server-accepted stream
            // close with `APP_ERR_UNSUPPORTED_MESSAGE`: the handler is a
            // request-only endpoint; responses belong on the client side.
            // `BatchStore` is a request the handler now implements (#648)
            // — it routes through `handle_batch_store`. Note an oversize
            // `BatchStore` (> `MAX_BATCH_STORE_HASHES`) never reaches this
            // match: `deserialize_batch_hashes` rejects it at the
            // `decode_message` step above, which maps to
            // `APP_ERR_MALFORMED_MESSAGE` (AC 19).
            match msg {
                wire::DhtMessage::FindNode(req) => Ok(DhtServerRequest::FindNode(req)),
                wire::DhtMessage::FindValue(req) => Ok(DhtServerRequest::FindValue(req)),
                wire::DhtMessage::Store(req) => Ok(DhtServerRequest::Store(req)),
                wire::DhtMessage::BatchStore(req) => Ok(DhtServerRequest::BatchStore(req)),
                wire::DhtMessage::FindNodeResponse(_)
                | wire::DhtMessage::FindValueResponse(_)
                | wire::DhtMessage::StoreAck(_)
                | wire::DhtMessage::BatchStoreAck(_) => {
                    reset(send, recv, APP_ERR_UNSUPPORTED_MESSAGE);
                    Err(DhtReadError {
                        err: anyhow::anyhow!("peer sent dht response on server stream"),
                        app_code: APP_ERR_UNSUPPORTED_MESSAGE,
                    })
                }
            }
        }
    }
}

/// Receiver-side wall-clock at microsecond resolution (ADR 022 §Content
/// Records and TTL: `expiry_us = receive_us + record_ttl_us` is anchored
/// on the receiver's wall-clock). `SystemTime::UNIX_EPOCH.elapsed()` is
/// used rather than `Instant::now()` because the value is stored in
/// records that survive process restarts (in a future persistence
/// extension) and a monotonic clock would not be comparable across
/// reboots.
///
/// On a wall-clock before-epoch (unset hardware clock) this returns 0;
/// the `RecordStore` then folds in its monotonic counter so subsequent
/// inserts still produce strictly-increasing keys.
fn now_us() -> u64 {
    // A wall-clock before UNIX_EPOCH means the operator's hardware
    // clock is mis-set — every record inserted while this is true
    // collapses to `receive_us = 0` and they all expire on the same
    // tick when the clock recovers. Surface it as an operator-visible
    // error (not a debug-level swallow) so the failure mode is
    // diagnosable from logs. The 0 fallback keeps the store
    // functional in the degraded state — `next_sequence` still
    // disambiguates LRU keys so admission never panics.
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
        Err(err) => {
            tracing::error!(
                error = %err,
                "wall-clock before UNIX_EPOCH; falling back to receive_us=0 — \
                 DHT records will expire en masse when the clock recovers. Set the host clock."
            );
            0
        }
    }
}

/// Lift the peer IP out of a connection's currently-selected path.
/// Mirrors the private `peer_ip` helper in [`crate::dispatch`] — same rationale:
/// relay-only connections have no IP key, so the per-IP rate-limit layer
/// is skipped for them.
fn peer_ip(conn: &Connection) -> Option<IpAddr> {
    let paths = conn.paths().peek().clone();
    paths
        .iter()
        .find(|p| p.is_selected() && !p.is_closed())
        .and_then(|p| match p.remote_addr() {
            TransportAddr::Ip(addr) => Some(addr.ip()),
            _ => None,
        })
        .or_else(|| {
            paths
                .iter()
                .filter(|p| !p.is_closed())
                .find_map(|p| match p.remote_addr() {
                    TransportAddr::Ip(addr) => Some(addr.ip()),
                    _ => None,
                })
        })
}
