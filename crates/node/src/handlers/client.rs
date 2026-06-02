//! `cdn/client/v1` handler — paid blob delivery (ADR 005 §`cdn/client/v1`).
//!
//! Serves the revenue path: a payer opens one bidirectional QUIC stream per
//! blob, the node answers with a signed [`StreamResponse`], then streams
//! [`ChunkData`] in `voucher_interval_mb`-sized batches, pausing at each batch
//! boundary to collect a cumulative payment `Voucher` before continuing, and
//! finishing with [`ClientMessage::StreamEnd`]. A delivery fault rides in the
//! initial response (`ok: false` + [`StreamError`]); a mid-stream voucher
//! rejection is sent as a [`ClientMessage::StreamError`] and the stream is
//! closed **cleanly** (no QUIC reset) so the client can read the reason.
//!
//! # Scope (#317 / #327)
//!
//! This handler validates vouchers only for channels present in the persisted
//! [`ChannelStateStore`]. Channels enter that set two ways: hydrated from the
//! store at construction (see [`ClientHandler::new`]), and live as the
//! on-chain `ChannelOpened` consumer in [`crate::payment_settlement`] (#327)
//! calls [`ClientHandler::register_open_channel`]. A voucher for a
//! still-unknown `channel_id` is rejected with
//! [`VoucherRejectReason::WrongChannel`] — the closest existing reason. After
//! accepting a voucher the handler emits a redeem hint (see
//! [`ClientHandler::attach_redeem_hint`]) so the settlement service can
//! withdraw the accrued claim once it crosses its threshold.
//!
//! # 0-RTT
//!
//! Unlike `cdn/probe/v1`, `cdn/client/v1` **rejects** 0-RTT (ADR 015): paid
//! accounting must not run on replayable early data, so `on_accepting` always
//! takes the full handshake.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, Hash};
use decdn_incentive::rate::{DEFAULT_TOLERANCE_BPS, RateError, verify_rate};
use decdn_incentive::store::StoreError;
use decdn_incentive::{
    ChannelId, ChannelState, ChannelStateStore, StreamSlashData, VoucherActivity, verify_binding,
    voucher_reject_reason, wire_voucher_to_signed,
};
use decdn_protocol::client::{
    ChunkData, ClientMessage, StreamError, StreamRequest, StreamRequestExt, StreamResponse,
    StreamResponseBody, VoucherRejectReason,
};
use decdn_protocol::{
    ALPN_CLIENT, APP_ERR_RATE_LIMITED, FrameError, MB_BYTES, decode_message, encode_message,
    read_frame, write_frame,
};
use futures_util::StreamExt as _;
use iroh::PublicKey;
use iroh::endpoint::{Accepting, Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::dispatch::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;

/// Default per-connection concurrent-stream cap for `cdn/client/v1` (ADR 005
/// §Concurrent stream limits). The QUIC transport config also caps bidi
/// streams at this value; the application semaphore makes the per-ALPN bound
/// explicit and testable.
pub const MAX_CLIENT_STREAMS: usize = 100;

// Per-stage timeouts so a stalled peer cannot pin a stream task indefinitely.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
const VOUCHER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const REJECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

// QUIC application error codes (ADR 013 §Application Error Codes). A clean
// voucher rejection does NOT use these — it writes a `StreamError` frame and
// finishes the stream so the reason survives.
const APP_ERR_NO_ERROR: u32 = 0x00;
const APP_ERR_MALFORMED_MESSAGE: u32 = 0x03;

/// Per-channel delivery state: the validated voucher state plus the channel-wide
/// cumulative byte counter that feeds voucher reconstruction (ADR 003 §Voucher
/// wire format — `bytes_delivered` is not on the wire).
#[derive(Debug)]
struct ChannelDeliveryState {
    state: ChannelState,
    /// Channel-wide cumulative bytes delivered as of the last accepted voucher.
    bytes_delivered_cumulative: U256,
}

/// `cdn/client/v1` paid-delivery handler.
pub struct ClientHandler {
    node_id: PublicKey,
    metrics: Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    eth_signer: Arc<PrivateKeySigner>,
    /// `SlashJudge` EIP-712 domain for `StreamResponse.slash_sig`.
    slash_domain: Eip712Domain,
    /// `PaymentChannel` EIP-712 domain for voucher verification.
    voucher_domain: Eip712Domain,
    /// `CapacityBond` EIP-712 domain for ephemeral `BindNodeId` verification.
    bind_domain: Eip712Domain,
    channel_state_store: Arc<dyn ChannelStateStore>,
    /// Per-channel state, hydrated from the store at construction. Outer mutex
    /// guards the map; each inner mutex serializes voucher application for one
    /// channel across its concurrent streams (ADR 003 §concurrent streams).
    channels: Arc<Mutex<HashMap<ChannelId, Arc<Mutex<ChannelDeliveryState>>>>>,
    /// Redeem-hint sender to the on-chain settlement service (#327), attached
    /// post-construction via [`ClientHandler::attach_redeem_hint`]. `None`
    /// until attached (e.g. in tests with no settlement service) — a hint is
    /// best-effort, so an unattached or full channel just skips it.
    redeem_hint: OnceLock<mpsc::Sender<ChannelId>>,
    /// In-memory last-voucher clock shared with `admin_v1_channels`
    /// (issue #749), attached post-construction via
    /// [`ClientHandler::attach_voucher_activity`]. `None` until attached
    /// (e.g. tests with no admin surface) — stamping is best-effort, so
    /// an unattached handler just skips it and the channel reports "no
    /// activity since restart" to the operator.
    voucher_activity: OnceLock<Arc<VoucherActivity>>,
    rate_per_mb: Arc<AtomicU64>,
    delivery_floor: u64,
    delivery_ceiling: u64,
    voucher_interval_mb: u64,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
}

impl std::fmt::Debug for ClientHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandler")
            .field("node_id", &self.node_id)
            .field("voucher_interval_mb", &self.voucher_interval_mb)
            .field("max_blob_size_bytes", &self.max_blob_size_bytes)
            .field("max_concurrent_streams", &self.max_concurrent_streams)
            .finish_non_exhaustive()
    }
}

impl ClientHandler {
    pub const ALPN: &'static [u8] = ALPN_CLIENT;

    /// Construct the handler, hydrating per-channel state from `channel_state_store`.
    ///
    /// # Errors
    ///
    /// Propagates a [`decdn_incentive::StoreError`] if the persisted channel
    /// state cannot be loaded — the node must not serve paid delivery without
    /// knowing prior voucher state (the #527 replay guard).
    #[allow(clippy::too_many_arguments)] // wiring struct; each arg is distinct runtime state.
    pub fn new(
        node_id: PublicKey,
        metrics: Arc<Metrics>,
        limiter: Arc<ConnectionLimiter>,
        cache: CacheEngine,
        eth_signer: Arc<PrivateKeySigner>,
        slash_domain: Eip712Domain,
        voucher_domain: Eip712Domain,
        bind_domain: Eip712Domain,
        channel_state_store: Arc<dyn ChannelStateStore>,
        rate_per_mb: Arc<AtomicU64>,
        delivery_floor: u64,
        delivery_ceiling: u64,
        voucher_interval_mb: u64,
        max_blob_size_bytes: u64,
        max_concurrent_streams: usize,
    ) -> anyhow::Result<Self> {
        let mut map = HashMap::new();
        for state in channel_state_store.load_all()? {
            let bytes = state.last_bytes_delivered();
            map.insert(
                state.channel_id,
                Arc::new(Mutex::new(ChannelDeliveryState {
                    state,
                    bytes_delivered_cumulative: bytes,
                })),
            );
        }
        Ok(Self {
            node_id,
            metrics,
            limiter,
            cache,
            eth_signer,
            slash_domain,
            voucher_domain,
            bind_domain,
            channel_state_store,
            channels: Arc::new(Mutex::new(map)),
            redeem_hint: OnceLock::new(),
            voucher_activity: OnceLock::new(),
            rate_per_mb,
            delivery_floor,
            delivery_ceiling,
            voucher_interval_mb,
            max_blob_size_bytes,
            max_concurrent_streams,
        })
    }

    /// Attach the redeem-hint sender from the on-chain settlement service
    /// (#327). Called once during runtime wiring; a second call is ignored
    /// (the `OnceLock` keeps the first). After this, an accepted voucher
    /// best-effort hints the channel for redemption.
    pub fn attach_redeem_hint(&self, tx: mpsc::Sender<ChannelId>) {
        let _ = self.redeem_hint.set(tx);
    }

    /// Attach the in-memory voucher-activity clock shared with
    /// `admin_v1_channels` (issue #749). Called once during runtime
    /// wiring; a second call is ignored (the `OnceLock` keeps the first).
    /// After this, each accepted voucher stamps the channel's last-voucher
    /// time so `decdn node channels` can report "time since last voucher".
    pub fn attach_voucher_activity(&self, activity: Arc<VoucherActivity>) {
        let _ = self.voucher_activity.set(activity);
    }

    /// Register a channel observed on-chain via `ChannelOpened` (#327) so the
    /// voucher path accepts vouchers for it. Persists a fresh [`ChannelState`]
    /// durably, then inserts it into the live map.
    ///
    /// **Idempotent:** a re-observed `ChannelOpened` (e.g. after a watcher
    /// resubscription) for an already-tracked channel is a no-op — it MUST
    /// NOT reset the accepted-voucher watermark and reopen the #527 replay
    /// window. The live map (hydrated from the store at construction, updated
    /// here) is the authority.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable persist fails; the watcher
    /// logs and retries on the next `ChannelOpened` observation.
    pub async fn register_open_channel(&self, state: ChannelState) -> Result<(), StoreError> {
        if self.channels.lock().await.contains_key(&state.channel_id) {
            return Ok(());
        }
        // The store write is a synchronous fsync (store trait §Durability) —
        // run it off the runtime worker, same as the voucher-accept path.
        let store = Arc::clone(&self.channel_state_store);
        let to_persist = state.clone();
        tokio::task::spawn_blocking(move || store.record(&to_persist))
            .await
            .map_err(|e| StoreError::Backend(format!("register_open_channel join: {e}")))??;

        let bytes = state.last_bytes_delivered();
        self.channels
            .lock()
            .await
            .entry(state.channel_id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(ChannelDeliveryState {
                    state,
                    bytes_delivered_cumulative: bytes,
                }))
            });
        Ok(())
    }

    /// Drop a settled channel (observed via `ChannelSettled`, #327) from the
    /// live map and the persisted store. Idempotent — forgetting an unknown
    /// channel is a no-op.
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable delete fails.
    pub async fn forget_channel(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        self.channels.lock().await.remove(&channel_id);
        let store = Arc::clone(&self.channel_state_store);
        tokio::task::spawn_blocking(move || store.forget(channel_id))
            .await
            .map_err(|e| StoreError::Backend(format!("forget_channel join: {e}")))?
    }

    /// Raise a tracked channel's on-chain deposit after a `ChannelToppedUp`
    /// event (#327). Without this, [`decdn_incentive::ChannelState::apply_voucher`]
    /// keeps enforcing the original (lower) deposit and rejects the
    /// otherwise-valid vouchers a client signs *after* topping up. No-op for
    /// channels this node does not track; idempotent for a non-increasing
    /// `new_deposit` (deposits only ever grow).
    ///
    /// # Errors
    ///
    /// Propagates a [`StoreError`] if the durable persist fails.
    pub async fn update_channel_deposit(
        &self,
        channel_id: ChannelId,
        new_deposit: U256,
    ) -> Result<(), StoreError> {
        let entry = self.channels.lock().await.get(&channel_id).cloned();
        let Some(entry) = entry else { return Ok(()) };
        let mut guard = entry.lock().await;
        if guard.state.deposit >= new_deposit {
            return Ok(());
        }
        // Persist the raised deposit before advancing in-memory, mirroring the
        // voucher-accept commit discipline (#527): record a clone durably and
        // swap it in only on success. The per-channel guard is held across the
        // blocking write so a concurrent voucher on this channel serializes.
        let mut next = guard.state.clone();
        next.deposit = new_deposit;
        let to_persist = next.clone();
        let store = Arc::clone(&self.channel_state_store);
        tokio::task::spawn_blocking(move || store.record(&to_persist))
            .await
            .map_err(|e| StoreError::Backend(format!("update_channel_deposit join: {e}")))??;
        guard.state = next;
        Ok(())
    }

    /// Accept the connection-level rate-limit permit, then serve each inbound
    /// bidi stream concurrently under a per-connection stream cap.
    ///
    /// Streams run as concurrent futures on this task (via `FuturesUnordered`)
    /// rather than `tokio::spawn`, because the iroh `ProtocolHandler::accept`
    /// signature borrows `&self` — spawned tasks would need a `'static` handle
    /// the trait does not hand us. Cooperative concurrency is sufficient for
    /// the I/O-bound delivery path.
    #[allow(clippy::cognitive_complexity)] // linear accept/select loop; splitting obscures it.
    async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        let _permit = match self.limiter.acquire(&conn) {
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

        let stream_sem = Arc::new(Semaphore::new(self.max_concurrent_streams));
        // Connection-scoped verified client binding (ADR 005 §Client identity
        // binding): the recovered Ethereum address is cached for the
        // connection's lifetime once a valid `BindNodeId` arrives.
        let bound_addr: Arc<Mutex<Option<Address>>> = Arc::new(Mutex::new(None));
        let client_node_id = B256::from(*conn.remote_id().as_bytes());

        let mut inflight = futures_util::stream::FuturesUnordered::new();
        loop {
            tokio::select! {
                biased;
                accepted = conn.accept_bi() => match accepted {
                    Ok((send, recv)) => {
                        let permit = Arc::clone(&stream_sem).try_acquire_owned().ok();
                        let bound = Arc::clone(&bound_addr);
                        inflight.push(self.serve_stream(send, recv, permit, bound, client_node_id));
                    }
                    // The connection closed (client done) or errored — stop
                    // accepting new streams. Not a handler fault.
                    Err(_) => break,
                },
                Some(res) = inflight.next(), if !inflight.is_empty() => {
                    if let Err(e) = res {
                        tracing::debug!(error = %e, "client stream ended with error");
                    }
                }
            }
        }
        // Drain any streams still finishing after the connection closed.
        while let Some(res) = inflight.next().await {
            if let Err(e) = res {
                tracing::debug!(error = %e, "client stream ended with error");
            }
        }
        Ok(())
    }

    /// Serve one delivery stream end to end.
    ///
    /// Kept as one linear, ADR-ordered sequence (read → bind → blob gate →
    /// channel → sign → deliver); splitting it would scatter the ADR-005
    /// ordering invariants across helpers — same rationale as the probe
    /// handler's `serve`.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    async fn serve_stream(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        permit: Option<OwnedSemaphorePermit>,
        bound_addr: Arc<Mutex<Option<Address>>>,
        client_node_id: B256,
    ) -> anyhow::Result<()> {
        let (req, ext) = match read_stream_request(&mut recv).await {
            Ok(pair) => pair,
            Err(StreamReadError { err, app_code }) => {
                reset_stream(&mut send, &mut recv, app_code);
                return Err(err);
            }
        };

        // Stream-cap exhausted: reset the stream with no signed response.
        // Signing a `StreamResponse` per rejected request would let a request
        // flood amplify into CPU exhaustion (an ECDSA signature per reject) —
        // the cap exists to shed load, not to add work to the reject path.
        if permit.is_none() {
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(());
        }

        // Verify an ephemeral client binding if present (ADR 005 §Client
        // identity binding) and remember the recovered address for the
        // connection's lifetime (a binding may be sent once and omitted on
        // later requests). A malformed binding is a client fault — reset.
        let mut verified_client: Option<Address> = *bound_addr.lock().await;
        if let Some(binding) = &ext.binding {
            let addr_bytes = binding.ethereum_address;
            match verify_binding(
                client_node_id,
                decdn_incentive::EPHEMERAL_BINDING_NONCE,
                &binding.binding_signature,
                &self.bind_domain,
            ) {
                Ok(recovered) if recovered.as_slice() == addr_bytes => {
                    verified_client = Some(recovered);
                    *bound_addr.lock().await = Some(recovered);
                }
                Ok(recovered) => {
                    tracing::warn!(
                        claimed = %Address::from(addr_bytes),
                        recovered = %recovered,
                        "client binding signature recovered a different address"
                    );
                    reset_stream(&mut send, &mut recv, APP_ERR_MALFORMED_MESSAGE);
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(error = %e, "client binding signature invalid");
                    reset_stream(&mut send, &mut recv, APP_ERR_MALFORMED_MESSAGE);
                    return Ok(());
                }
            }
        }

        let hash = Hash::from_bytes(req.hash);

        // Blob availability gate. A store fault is NOT an absence: `Ok(false)`
        // means the node genuinely lacks the blob (NotFound / EvictedSinceProbe),
        // but `Err` is a transient local store failure that must not masquerade
        // as a signed `NotFound` — a paying client would treat that as
        // authoritative and stop asking. Surface it as `InternalError` and log.
        match self.cache.has(hash).await {
            Ok(true) => {}
            Ok(false) => {
                let err = if self.cache.is_evicted(hash) {
                    StreamError::EvictedSinceProbe
                } else {
                    StreamError::NotFound
                };
                return self.respond_error(&mut send, &req, err).await;
            }
            Err(e) => {
                tracing::warn!(%hash, error = %e, "cache `has` lookup failed on delivery path");
                return self
                    .respond_error(&mut send, &req, StreamError::InternalError)
                    .await;
            }
        }

        // Size gate. `has` just confirmed the blob is present and complete, so an
        // `inspect` error — or a `None` size (a `Partial`/`NotFound` status) — is
        // a real store fault, NOT a zero-length blob. Advertising `total_bytes: 0`
        // for a non-empty blob would sign a `StreamResponse` the delivery then
        // contradicts, and the receiver (expecting 0 bytes) would abort on the
        // first chunk. Surface the fault instead; only a genuinely complete,
        // zero-length blob yields `total_bytes == 0`.
        let size = match self.cache.inspect(hash).await {
            Ok(preview) => preview.size_bytes,
            Err(e) => {
                tracing::warn!(%hash, error = %e, "cache `inspect` failed on delivery path");
                return self
                    .respond_error(&mut send, &req, StreamError::InternalError)
                    .await;
            }
        };
        let Some(total_bytes) = size else {
            tracing::warn!(
                %hash,
                "blob present per `has` but `inspect` reports no size; treating as fault"
            );
            return self
                .respond_error(&mut send, &req, StreamError::InternalError)
                .await;
        };
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            return self
                .respond_error(&mut send, &req, StreamError::BlobTooLarge)
                .await;
        }

        // Resolve the channel (must be pre-persisted — see module docs / #327).
        let channel_id = ChannelId::from(req.channel_id);
        let channel = self.channels.lock().await.get(&channel_id).cloned();

        // A verified client binding MUST match the channel's authorized client.
        // Otherwise this connection is requesting paid delivery on a channel it
        // does not own (its vouchers would fail `WrongSigner` regardless) — so
        // refuse before delivering any bytes, closing the leech for bound
        // clients. Unbound connections fall back to the voucher-signature gate;
        // an on-chain NodeId→address lookup that would close the residual for
        // unbound peers is out of scope (#327).
        if let (Some(client), Some(chan)) = (verified_client, channel.as_ref()) {
            let owner = chan.lock().await.state.client;
            if client != owner {
                tracing::warn!(%client, %owner, "binding does not authorize this channel");
                return self
                    .respond_error(&mut send, &req, StreamError::NotFound)
                    .await;
            }
        }

        // Honor a client voucher-interval proposal (ADR 003 §Voucher Interval
        // Negotiation): accept the smaller of the proposal and our configured
        // cadence, never below 1 MB.
        let interval_mb = match ext.voucher_interval_mb {
            Some(proposed) => self.voucher_interval_mb.min(proposed).max(1),
            None => self.voucher_interval_mb,
        };

        // Build and sign the success response.
        let rate_per_mb = self.clamped_rate();
        let body = StreamResponseBody {
            hash: req.hash,
            ok: true,
            rate_per_mb,
            total_bytes,
            channel_id: req.channel_id,
            timestamp_us: req.timestamp_us,
            redirect: None,
        };
        let resp = self.sign_response(body, None, Some(interval_mb))?;
        self.write_message(&mut send, &ClientMessage::StreamResponse(resp))
            .await?;

        // Stream the blob, collecting vouchers at each interval boundary.
        self.deliver(
            &mut send,
            &mut recv,
            hash,
            req.byte_offset,
            channel_id,
            channel.as_ref(),
            rate_per_mb,
            interval_mb,
        )
        .await
    }

    /// Stream blob bytes in `voucher_interval_mb`-sized batches, pausing to
    /// collect a cumulative voucher at each boundary and a closing voucher for
    /// the final partial batch. Returns `Ok(())` on a clean rejection or a
    /// completed delivery.
    #[allow(clippy::too_many_arguments)]
    async fn deliver(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        byte_offset: u64,
        channel_id: ChannelId,
        channel: Option<&Arc<Mutex<ChannelDeliveryState>>>,
        rate_per_mb: u64,
        interval_mb: u64,
    ) -> anyhow::Result<()> {
        let blob = self
            .cache
            .get(hash)
            .await
            .map_err(|e| anyhow::anyhow!("cache get failed: {e}"))?;
        let offset = usize::try_from(byte_offset)
            .unwrap_or(usize::MAX)
            .min(blob.len());
        let data = blob.slice(offset..);

        let interval_bytes = interval_mb.saturating_mul(MB_BYTES);
        let mut unvouchered: u64 = 0;

        for chunk in data.chunks(decdn_protocol::CHUNK_SIZE) {
            self.write_message(
                send,
                &ClientMessage::ChunkData(ChunkData {
                    bytes: chunk.to_vec(),
                }),
            )
            .await?;
            unvouchered = unvouchered.saturating_add(chunk.len() as u64);
            if unvouchered >= interval_bytes {
                match self
                    .collect_voucher(send, recv, channel_id, channel, rate_per_mb, unvouchered)
                    .await?
                {
                    VoucherOutcome::Accepted => unvouchered = 0,
                    VoucherOutcome::Rejected => return Ok(()),
                }
            }
        }
        // Closing voucher for the final partial batch.
        if unvouchered > 0
            && matches!(
                self.collect_voucher(send, recv, channel_id, channel, rate_per_mb, unvouchered)
                    .await?,
                VoucherOutcome::Rejected
            )
        {
            return Ok(());
        }

        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        Ok(())
    }

    /// Read and apply one cumulative voucher covering `delta_bytes` of newly
    /// delivered bytes. A permanent voucher rejection writes a `StreamError` and
    /// finishes the stream cleanly (no reset). A transient store-write failure
    /// is likewise surfaced cleanly as `VoucherRejectReason::RetryLater` so the
    /// client resends the same voucher on a fresh stream (ADR 003 §332). Only an
    /// underpayment fails the stream: no wire reason exists for it, and the
    /// client is blocked awaiting `VoucherAck` so it cannot resend mid-stream.
    #[allow(clippy::too_many_arguments)]
    async fn collect_voucher(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        channel_id: ChannelId,
        channel: Option<&Arc<Mutex<ChannelDeliveryState>>>,
        rate_per_mb: u64,
        delta_bytes: u64,
    ) -> anyhow::Result<VoucherOutcome> {
        let wire = read_voucher(recv).await?;

        // Unknown channel (#327 boundary): reject with the closest reason.
        let Some(channel) = channel else {
            self.write_reject(send, VoucherRejectReason::WrongChannel)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        };

        let mut guard = channel.lock().await;

        // Expiry gate (#327): once the channel has passed its on-chain
        // `expiresAt`, `withdraw`/`closeChannel` revert and the client can
        // `reclaimExpired` for a full refund — any further delivery would be
        // unpaid. Refuse rather than accept a voucher we could never redeem.
        // (The settlement sweep normally closes + retires channels well before
        // this; this is the defense-in-depth for a node that was down through
        // the close window.) Surface it in-band as `VoucherRejectReason::Expired`
        // and finish the stream cleanly (#751) so the client sees an actionable
        // reason instead of an opaque connection drop.
        if crate::payment_settlement::is_expired(
            crate::payment_settlement::unix_now(),
            guard.state.expires_at,
        ) {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::Expired)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        }

        let new_bytes = guard
            .bytes_delivered_cumulative
            .saturating_add(U256::from(delta_bytes));
        let amount = U256::from_be_bytes(wire.amount);
        let amount_delta = amount.saturating_sub(guard.state.last_amount());

        // Rate enforcement (ADR 003 §Voucher withholding). There is no wire
        // reason code for underpayment, so an underpaying voucher fails the
        // stream rather than looping — looping would deadlock, since the client
        // is blocked awaiting `VoucherAck` and cannot send a corrected voucher.
        if let Err(RateError::Underpayment { .. }) = verify_rate(
            amount_delta,
            U256::from(delta_bytes),
            rate_per_mb,
            DEFAULT_TOLERANCE_BPS,
        ) {
            drop(guard);
            anyhow::bail!("voucher underpays for {delta_bytes} delivered bytes");
        }

        let Ok(signed) = wire_voucher_to_signed(&wire, channel_id, guard.state.token, new_bytes)
        else {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::BadSignature)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        };

        // `apply_voucher` performs a synchronous fsynced redb write (store
        // trait §Durability), which must not block a runtime worker — run it on
        // the blocking pool against a clone. The per-channel guard is held
        // across the await so same-channel streams still serialize (ADR 003
        // §concurrent streams); the clone is committed back to in-memory state
        // only on `Ok`, preserving the strict-durability invariant (#527).
        let mut candidate = guard.state.clone();
        let signed_c = signed.clone();
        let domain = self.voucher_domain.clone();
        let store = Arc::clone(&self.channel_state_store);
        let (apply_res, candidate) = tokio::task::spawn_blocking(move || {
            let res = candidate.apply_voucher(&signed_c, &domain, &*store);
            (res, candidate)
        })
        .await
        .map_err(|e| anyhow::anyhow!("voucher apply task failed: {e}"))?;

        match apply_res {
            Ok(applied) => {
                guard.state = candidate;
                guard.bytes_delivered_cumulative = new_bytes;
                drop(guard);
                // Stamp the in-memory last-voucher clock for
                // `admin_v1_channels` (issue #749). Best-effort: an
                // unattached clock (no admin surface) just skips. Done
                // after the guard drop — the activity map has its own
                // lock and doesn't need the per-channel guard.
                if let Some(activity) = self.voucher_activity.get() {
                    activity.touch(channel_id);
                }
                // Nonce-gap signal (#747): the voucher was accepted, but its
                // nonce skipped values past the prior `last_nonce + 1`. The
                // structured `tracing::warn!` already fired inside
                // `apply_voucher`; here we surface the rate to operators via
                // `decdn_voucher_nonce_gaps_total` for alerting.
                if applied.is_gapped() {
                    self.metrics.voucher_nonce_gap();
                }
                // Hint the on-chain settlement service that this channel's
                // accrued claim advanced (#327). Best-effort: an unattached or
                // full hint channel just skips — the next voucher re-hints, the
                // redeemer self-tick sweeps, and shutdown closes any residual
                // claim. Only `Full` is counted (a saturated queue is a real
                // dropped hint; a sustained rate is the signal worth watching,
                // #751). `Closed` — the redeemer aborted during shutdown
                // `quiesce_redeemer` — is expected, not a fault, so it is
                // deliberately left uncounted; don't "fix" this to count both.
                if let Some(tx) = self.redeem_hint.get()
                    && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) =
                        tx.try_send(channel_id)
                {
                    self.metrics.redeem_hint_dropped();
                }
                self.write_message(send, &ClientMessage::VoucherAck).await?;
                Ok(VoucherOutcome::Accepted)
            }
            Err(e) => {
                drop(guard);
                if let Ok(reason) = voucher_reject_reason(&e) {
                    self.write_reject(send, reason).await?;
                    Ok(VoucherOutcome::Rejected)
                } else {
                    // Transient store failure (#527, `RetrySignal`): in-memory
                    // state did not advance. Surface it in-band as `RetryLater`
                    // and finish the stream cleanly (MUST NOT `VoucherAck`, ADR
                    // 003 §332) so the client resends the same voucher on a
                    // fresh stream rather than seeing an opaque connection drop.
                    tracing::warn!(error = %e, "channel store write failed; rejecting with RetryLater");
                    self.write_reject(send, VoucherRejectReason::RetryLater)
                        .await?;
                    Ok(VoucherOutcome::Rejected)
                }
            }
        }
    }

    /// Load the configured rate and clamp it to the delivery bounds before
    /// signing a `StreamResponse`, logging a warning and incrementing
    /// `rate_bounds_clamp_events` on any clamp (ADR 005 §Rate bounds — the same
    /// clamp-and-warn the probe handler applies before signing a `ProbeResponse`).
    fn clamped_rate(&self) -> u64 {
        let raw_rate = self.rate_per_mb.load(Ordering::Relaxed);
        let rate_per_mb = raw_rate.clamp(self.delivery_floor, self.delivery_ceiling);
        if rate_per_mb != raw_rate {
            self.metrics.rate_bounds_clamped();
            tracing::warn!(
                raw_rate,
                clamped = rate_per_mb,
                floor = self.delivery_floor,
                ceiling = self.delivery_ceiling,
                "rate_per_mb clamped to delivery bounds before signing StreamResponse"
            );
        }
        rate_per_mb
    }

    /// Sign a `StreamResponse` body and assemble the full message.
    fn sign_response(
        &self,
        body: StreamResponseBody,
        error: Option<StreamError>,
        voucher_interval_mb: Option<u64>,
    ) -> anyhow::Result<StreamResponse> {
        let slash_sig = StreamSlashData::from_response_body(&body)
            .sign(self.eth_signer.as_ref(), &self.slash_domain)
            .map_err(|e| anyhow::anyhow!("stream slash_sig signing failed: {e}"))?
            .as_bytes()
            .to_vec();
        Ok(StreamResponse {
            body,
            error,
            voucher_interval_mb,
            slash_sig,
        })
    }

    /// Send a signed `StreamResponse { ok: false, error }` (delivery-side
    /// failure), then finish the stream.
    async fn respond_error(
        &self,
        send: &mut SendStream,
        req: &StreamRequest,
        error: StreamError,
    ) -> anyhow::Result<()> {
        let rate_per_mb = self.clamped_rate();
        let body = StreamResponseBody {
            hash: req.hash,
            ok: false,
            rate_per_mb,
            total_bytes: 0,
            channel_id: req.channel_id,
            timestamp_us: req.timestamp_us,
            redirect: None,
        };
        let resp = self.sign_response(body, Some(error), None)?;
        self.write_message(send, &ClientMessage::StreamResponse(resp))
            .await?;
        let _ = send.finish();
        Ok(())
    }

    /// Write a mid-stream `StreamError { VoucherRejected }` and finish the
    /// stream **cleanly** — no QUIC reset — so the client can read the reason
    /// (ADR 005 §`VoucherRejected` semantics).
    async fn write_reject(
        &self,
        send: &mut SendStream,
        reason: VoucherRejectReason,
    ) -> anyhow::Result<()> {
        self.write_message(
            send,
            &ClientMessage::StreamError(StreamError::VoucherRejected { reason }),
        )
        .await?;
        let _ = send.finish();
        Ok(())
    }

    async fn write_message(
        &self,
        send: &mut SendStream,
        msg: &ClientMessage,
    ) -> anyhow::Result<()> {
        let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("encode failed: {e}"))?;
        write_frame(send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("write failed: {e}"))
    }
}

/// Outcome of a single batch-boundary voucher exchange.
enum VoucherOutcome {
    Accepted,
    Rejected,
}

impl ProtocolHandler for ClientHandler {
    /// ADR 015: `cdn/client/v1` MUST reject 0-RTT — always take the full
    /// handshake, never `into_0rtt()`. This is the inverse of `ProbeHandler`.
    async fn on_accepting(&self, accepting: Accepting) -> Result<Connection, AcceptError> {
        accepting.await.map_err(AcceptError::from_err)
    }

    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.serve(connection)
            .await
            .map_err(|e| AcceptError::from_err(std::io::Error::other(e.to_string())))
    }
}

/// Error from the request read path carrying the ADR 013 app error code to
/// propagate to the peer.
struct StreamReadError {
    err: anyhow::Error,
    app_code: u32,
}

const fn frame_err_code(e: &FrameError) -> u32 {
    match e {
        FrameError::TooLarge(_) => 0x02,
        FrameError::Io(_) => APP_ERR_NO_ERROR,
        FrameError::Varint | FrameError::Decode(_) => APP_ERR_MALFORMED_MESSAGE,
    }
}

fn reset_stream(send: &mut SendStream, recv: &mut RecvStream, code: u32) {
    let v = VarInt::from_u32(code);
    let _ = send.reset(v);
    let _ = recv.stop(v);
}

/// Read one framed [`ClientMessage::StreamRequest`] with a timeout, returning
/// the base request plus its [`StreamRequestExt`] parsed from the trailing
/// bytes (the ADR 005 two-phase pattern). An absent extension yields
/// `StreamRequestExt::default()`.
async fn read_stream_request(
    recv: &mut RecvStream,
) -> Result<(StreamRequest, StreamRequestExt), StreamReadError> {
    let frame = match tokio::time::timeout(REQUEST_READ_TIMEOUT, read_frame(recv)).await {
        Err(_) => {
            return Err(StreamReadError {
                err: anyhow::anyhow!("stream request timed out after {REQUEST_READ_TIMEOUT:?}"),
                app_code: APP_ERR_NO_ERROR,
            });
        }
        Ok(Err(e)) => {
            let app_code = frame_err_code(&e);
            return Err(StreamReadError {
                err: anyhow::anyhow!("stream request frame read failed: {e}"),
                app_code,
            });
        }
        Ok(Ok(frame)) => frame,
    };
    match decode_message::<ClientMessage>(&frame) {
        Ok((ClientMessage::StreamRequest(req), remainder)) => {
            let ext = decdn_protocol::parse_stream_request_ext(remainder).map_err(|e| {
                StreamReadError {
                    err: anyhow::anyhow!("stream request ext decode failed: {e}"),
                    app_code: APP_ERR_MALFORMED_MESSAGE,
                }
            })?;
            Ok((req, ext))
        }
        Ok((_, _)) => Err(StreamReadError {
            err: anyhow::anyhow!("expected ClientMessage::StreamRequest"),
            app_code: 0x01, // UNSUPPORTED_MESSAGE
        }),
        Err(e) => Err(StreamReadError {
            err: anyhow::anyhow!("stream request decode failed: {e}"),
            app_code: APP_ERR_MALFORMED_MESSAGE,
        }),
    }
}

/// Read one framed [`ClientMessage::Voucher`] with a timeout.
async fn read_voucher(recv: &mut RecvStream) -> anyhow::Result<decdn_protocol::client::Voucher> {
    let frame = tokio::time::timeout(VOUCHER_READ_TIMEOUT, read_frame(recv))
        .await
        .map_err(|_| anyhow::anyhow!("voucher read timed out after {VOUCHER_READ_TIMEOUT:?}"))?
        .map_err(|e| anyhow::anyhow!("voucher frame read failed: {e}"))?;
    match decode_message::<ClientMessage>(&frame) {
        Ok((ClientMessage::Voucher(v), _)) => Ok(v),
        Ok((_, _)) => anyhow::bail!("expected ClientMessage::Voucher"),
        Err(e) => anyhow::bail!("voucher decode failed: {e}"),
    }
}
