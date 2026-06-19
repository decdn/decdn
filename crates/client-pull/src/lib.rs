//! Reusable `cdn/client/v1` paid-pull requester, shared by the node
//! (node-to-node miss pulls, #317) and the CLI (client fetch / bundle pull).
//!
//! [`stream_fetch`] performs one full delivery exchange against a remote node:
//! it sends a [`StreamRequest`], validates and verifies the signed
//! [`StreamResponse`], receives `ChunkData` while paying cumulative vouchers
//! at each `voucher_interval_mb` boundary, and returns the assembled blob on
//! `StreamEnd`. It is the receive-side call site for the #252 rule (reject a
//! `rate_per_mb == 0` response) and the `slash_sig` verification obligation
//! (ADR 014 §1).
//!
//! Mirrors `cli::commands::probe_client::probe_once` in spirit, but for the
//! paid path: it signs vouchers (so it needs the incentive layer and a signer)
//! and, unlike probe, **never** attempts 0-RTT (ADR 015 forbids 0-RTT on
//! `cdn/client/v1`).
//!
//! # Scope
//!
//! - **Redirects** (`StreamResponse.redirect`) are detected and rejected, not
//!   followed: resolving a redirect `NodeId` to a dialable address needs the
//!   provider-discovery layer (ADR 001 / 022), which is out of scope. A #317
//!   server always sends `redirect: None`.
//! - **Whole-blob BLAKE3 verification** runs only for a full fetch
//!   (`byte_offset == 0`); a resumed fetch cannot recompute the whole-blob hash
//!   from a suffix (bao tree-hash verification is out of scope).

/// Buyer-side `PaymentChannel` open kernel (#940), shared by the node service
/// and the CLI.
pub mod buyer_channel;
mod ledger;

pub use ledger::{ChannelLedger, Cumulative};

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::local::PrivateKeySigner;
use bytes::{Bytes, BytesMut};
use decdn_incentive::{BuyerChannelState, StreamSlashData, Voucher, signed_to_wire_voucher};
use decdn_protocol::client::{
    ClientMessage, StreamError, StreamRequest, StreamResponse, VoucherRejectReason,
};
use decdn_protocol::{
    ALPN_CLIENT, DEFAULT_VOUCHER_INTERVAL_MB, MB_BYTES, decode_message, encode_message, read_frame,
    write_frame,
};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};

/// Per-channel context the requester needs to sign vouchers.
///
/// The `prior_*` fields capture the channel's cumulative state from earlier
/// streams so a **reused** channel resumes correctly: vouchers are cumulative
/// across the channel's lifetime, so the node's `last_nonce` /
/// `last_bytes_delivered` / `last_amount` are non-zero after the first stream.
/// Starting a fresh stream from zero would be rejected (`StaleNonce` /
/// `BytesRegression`). For a brand-new channel pass `U256::ZERO` for all three.
#[derive(Clone)]
pub struct ChannelContext {
    /// On-chain `channelId`.
    pub channel_id: B256,
    /// `ERC-20` token bound by the channel (`USDC`).
    pub token: Address,
    /// On-chain deposit (informational here; the node enforces it).
    pub deposit: U256,
    /// Client key that signs vouchers.
    pub client_signer: Arc<PrivateKeySigner>,
    /// `PaymentChannel` EIP-712 domain.
    pub voucher_domain: Eip712Domain,
    /// Nonce of the last voucher the client issued on this channel (the next
    /// voucher uses `prior_nonce + 1`). `ZERO` for a fresh channel.
    pub prior_nonce: U256,
    /// Cumulative bytes paid for on this channel before this stream.
    pub prior_bytes_delivered: U256,
    /// Cumulative amount paid on this channel before this stream.
    pub prior_amount: U256,
}

impl ChannelContext {
    /// Build a context for a buyer-held channel, resuming from its persisted
    /// cumulative voucher state (#744). The `prior_*` fields come straight from
    /// the stored [`BuyerChannelState`], so the next voucher continues the
    /// channel at `last_nonce + 1` rather than restarting from zero (which the
    /// upstream node would reject). For a freshly-opened channel the stored
    /// `last_*` are all `ZERO`, yielding a fresh-channel context.
    #[must_use]
    pub const fn for_buyer_channel(
        state: &BuyerChannelState,
        client_signer: Arc<PrivateKeySigner>,
        voucher_domain: Eip712Domain,
    ) -> Self {
        Self {
            channel_id: state.channel_id,
            token: state.token,
            deposit: state.deposit,
            client_signer,
            voucher_domain,
            prior_nonce: state.last_nonce,
            prior_bytes_delivered: state.last_bytes_delivered,
            prior_amount: state.last_amount,
        }
    }
}

impl std::fmt::Debug for ChannelContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelContext")
            .field("channel_id", &self.channel_id)
            .field("token", &self.token)
            .field("deposit", &self.deposit)
            .finish_non_exhaustive()
    }
}

/// The channel's acked voucher watermark, threaded through [`stream_fetch_tracked`]
/// as an out-param so the caller can persist what it paid (#852).
///
/// [`stream_fetch_tracked`] drives the pull through a one-shot [`ChannelLedger`]
/// seeded from the channel's prior cumulative state, then copies the ledger's
/// acked cumulative back into this watermark (`set_from_cumulative`) before
/// returning — so it always holds the
/// **absolute** cumulative totals of the last *acked* voucher (not per-stream
/// deltas), exactly the triple `BuyerChannelService::record_progress` expects.
/// Because the copy-back runs on every return path (including the `Err`/timeout
/// arms), the latest acked totals survive a mid-stream failure or a
/// paid-but-corrupt delivery, recorded against the upstream's committed
/// watermark (ADR 003).
///
/// **Acked only.** The watermark tracks vouchers the upstream *acknowledged*. If
/// the upstream commits a voucher (ADR 003: commit precedes the ack) but the ack
/// is then lost — a dropped connection while reading it — the watermark lags by
/// that one voucher; the next reuse re-signs a stale nonce and is rejected until
/// the channel rotates. That residual is inherent to one-sided ack loss and is
/// not closed here (it would need a reconciliation read of the upstream's
/// committed nonce on the next open).
///
/// Read the persistable totals via [`VoucherProgress::acked`], which yields `None`
/// when nothing was acked on this stream (so there is nothing to persist).
#[derive(Clone, Copy, Debug, Default)]
pub struct VoucherProgress {
    /// Nonce of the last acked voucher (the channel's prior nonce until the first
    /// ack on this stream).
    nonce: U256,
    /// Cumulative channel bytes paid for as of the last acked voucher.
    bytes_delivered: U256,
    /// Cumulative channel amount paid as of the last acked voucher.
    amount: U256,
    /// Count of vouchers acked on this stream.
    vouchers_sent: u64,
}

impl VoucherProgress {
    /// Seed the watermark from the channel's prior cumulative state (the last
    /// voucher acked on earlier streams). `vouchers_sent` starts at `0` — it
    /// counts acks on *this* stream.
    const fn seed(ctx: &ChannelContext) -> Self {
        Self {
            nonce: ctx.prior_nonce,
            bytes_delivered: ctx.prior_bytes_delivered,
            amount: ctx.prior_amount,
            vouchers_sent: 0,
        }
    }

    /// Set the watermark from a ledger [`Cumulative`] plus the channel's seed
    /// nonce. `vouchers_sent` is set to the number of vouchers acked since the
    /// seed (`cum.nonce - prior_nonce`, saturated to `u64`); `acked()` only checks
    /// it is `> 0`, so this preserves the "acked iff the nonce advanced past the
    /// seed" contract even when the ledger was shared across concurrent streams.
    fn set_from_cumulative(&mut self, cum: Cumulative, prior_nonce: U256) {
        self.nonce = cum.nonce;
        self.bytes_delivered = cum.bytes;
        self.amount = cum.amount;
        self.vouchers_sent =
            u64::try_from(cum.nonce.saturating_sub(prior_nonce)).unwrap_or(u64::MAX);
    }

    /// The cumulative `(nonce, bytes_delivered, amount)` to persist via
    /// `record_progress`, or `None` if no voucher was acked on this stream
    /// (nothing new was paid, so there is nothing to record).
    #[must_use]
    pub fn acked(&self) -> Option<(U256, U256, U256)> {
        (self.vouchers_sent > 0).then_some((self.nonce, self.bytes_delivered, self.amount))
    }
}

/// The upstream delivered bytes whose whole-blob BLAKE3 hash did not match the
/// requested content hash — a paid-but-corrupt delivery (the content-addressing
/// invariant, ADR 014). Returned (via `anyhow`) by [`stream_fetch`] so callers
/// can `downcast_ref` to classify corruption (e.g. a reputation `Corruption`
/// outcome) without matching on the error message string. The `Display` text is
/// kept stable for logs and the existing requester tests.
#[derive(Debug)]
pub struct HashMismatch;

impl std::fmt::Display for HashMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("received bytes do not match requested hash")
    }
}

impl std::error::Error for HashMismatch {}

/// Typed sentinel for a server that claimed a `total_bytes` above the buyer's
/// `max_blob_size_bytes` ceiling (#840). Returned (not a bare string) so the
/// pull orchestrator can `downcast_ref` and classify it as a buyer-side policy
/// rejection — distinct from a hash mismatch or an unreachable peer — rather
/// than mis-attributing it to the provider's reputation. `Display` carries
/// `BlobTooLarge` so logs and the existing requester tests can match on it.
#[derive(Debug)]
pub struct BlobTooLargeClaim {
    pub claimed: u64,
    pub ceiling: u64,
}

impl std::fmt::Display for BlobTooLargeClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server claimed {} bytes, exceeding max_blob_size {} bytes (BlobTooLarge)",
            self.claimed, self.ceiling
        )
    }
}

impl std::error::Error for BlobTooLargeClaim {}

/// Typed sentinel for the buyer's own per-candidate pull deadline firing (#857).
/// Returned (not a bare string) so the pull orchestrator can `downcast_ref` and
/// recognize that the timeout is OUR local deadline — a possibly mis-sized
/// configuration value — not evidence the provider is unreachable, and so must
/// not tar the provider's reputation locally or over gossip. `Display` keeps the
/// stable `timed out` text for logs (and for the `!contains("timed out")`
/// negative assertion in `node_to_node_pull_through`'s deadline test).
#[derive(Debug)]
pub struct PullTimeout {
    pub after: Duration,
}

impl std::fmt::Display for PullTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream_fetch timed out after {:?}", self.after)
    }
}

impl std::error::Error for PullTimeout {}

/// Typed sentinel for the upstream rejecting a voucher we presented mid-stream
/// (#857) — e.g. a stale nonce (#852), deposit exhaustion, or a wrong-channel
/// mismatch. This is OUR payment-side fault, not the provider's, so the pull
/// orchestrator `downcast_ref`s it to skip the candidate WITHOUT recording a
/// reputation observation (mirroring the buyer channel-open-failure arm). Named
/// with the `Upstream` prefix to disambiguate from the protocol-level
/// `StreamError::VoucherRejected` reason enum, whose `reason` it carries verbatim
/// (the `Copy` `VoucherRejectReason`, not a lossy stringification) so a future
/// caller can branch on retry-vs-top-up-vs-abandon without re-parsing a message.
/// `Display` keeps the stable `voucher rejected` text for logs.
#[derive(Debug)]
pub struct UpstreamVoucherRejected {
    pub reason: VoucherRejectReason,
}

impl std::fmt::Display for UpstreamVoucherRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `{:?}` rendering of the reason is load-bearing: the loopback tests
        // assert `.contains("RetryLater")` / `.contains("Expired")` on this string.
        // A custom `Display` for `VoucherRejectReason` would have to reproduce the
        // variant names verbatim, so keep the Debug rendering here.
        write!(f, "voucher rejected: {:?}", self.reason)
    }
}

impl std::error::Error for UpstreamVoucherRejected {}

/// Fetch `hash` from `target` over `cdn/client/v1`, paying as bytes arrive.
///
/// `expected_signer` is the delivering node's Ethereum address, used to verify
/// the response `slash_sig`. `byte_offset` resumes a partial fetch. Use
/// [`stream_fetch_tracked`] instead if you need to persist the voucher watermark
/// the channel reached (#852); this convenience wrapper discards it.
///
/// # Errors
///
/// Fails on connect/transport errors, an invalid or zero-rate response, a
/// `slash_sig` that does not recover to `expected_signer`, a mismatched echoed
/// field, a mid-stream `VoucherRejected`, a hash mismatch, or a timeout.
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    timeout: Duration,
) -> anyhow::Result<Bytes> {
    stream_fetch_tracked(
        endpoint,
        target,
        ctx,
        slash_domain,
        expected_signer,
        hash,
        byte_offset,
        timestamp_us,
        timeout,
        // No buyer-side blob-size ceiling on this test/loopback helper. The
        // production pull path does not go through here — it calls
        // `stream_fetch_tracked` directly (`node_origin::pull_from_candidate`)
        // with its configured `max_blob_size_bytes`.
        0,
        &mut VoucherProgress::default(),
    )
    .await
}

/// Like [`stream_fetch`], but reports the channel's acked voucher watermark via
/// the `progress` out-param so the caller can persist what it paid (#852).
///
/// `progress` is an out-param: on return it holds the cumulative `(nonce,
/// bytes_delivered, amount)` of the last *acked* voucher. Internally the pull
/// runs against a one-shot [`ChannelLedger`] seeded from `ctx.prior_*`; the
/// ledger's snapshot is copied back into `progress` on every return path — `Ok`,
/// `Err`, or timeout — so the caller can record progress even for a mid-stream
/// failure or a paid-but-corrupt delivery. See [`VoucherProgress`].
///
/// # Errors
///
/// Same as [`stream_fetch`].
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_tracked(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    timeout: Duration,
    max_blob_size_bytes: u64,
    progress: &mut VoucherProgress,
) -> anyhow::Result<Bytes> {
    // One-shot ledger seeded from the channel's prior cumulative state. A single
    // (non-shared) pull owns its ledger; concurrent shared-channel pulls use
    // `stream_fetch_shared` with a caller-owned ledger instead.
    let ledger = ChannelLedger::new(Cumulative {
        nonce: ctx.prior_nonce,
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    });
    let result = tokio::time::timeout(
        timeout,
        fetch_inner(
            endpoint,
            target,
            ctx,
            slash_domain,
            expected_signer,
            hash,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            &ledger,
        ),
    )
    .await
    .map_err(|_| anyhow::Error::new(PullTimeout { after: timeout }));
    // Copy the acked watermark back into `progress` on EVERY return path (Ok, Err,
    // timeout) BEFORE returning, so the latest acked totals survive a mid-stream
    // failure or a paid-but-corrupt delivery (#852). The ledger commits only after
    // an ack, so its snapshot is exactly the last acked cumulative.
    progress.set_from_cumulative(ledger.snapshot().await, ctx.prior_nonce);
    // Flatten: outer `Result` is the timeout error, inner is `fetch_inner`'s.
    result?
}

/// Like [`stream_fetch`], but issues vouchers through a caller-owned shared
/// [`ChannelLedger`] so multiple concurrent pulls on ONE payment channel coordinate.
///
/// The bug this fixes: each `stream_fetch`/`stream_fetch_tracked` call seeds its
/// own voucher state from `ctx.prior_*`, so N concurrent pulls on the same channel
/// all sign the next voucher at `prior_nonce + 1` and collide — the node accepts
/// exactly one and rejects the rest as `StaleNonce`. Passing every concurrent
/// caller the SAME `&ChannelLedger` (typically an `Arc<ChannelLedger>` shared
/// across `tokio::spawn`/`join!`) serializes their voucher issuance through the
/// ledger's mutex: each issues the next nonce in turn, the channel advances
/// monotonically, and all pulls succeed.
///
/// The caller owns the ledger's lifetime and reads its final cumulative via
/// [`ChannelLedger::snapshot`] to persist what the channel paid (this entrypoint
/// does not surface a [`VoucherProgress`] — the shared ledger IS the watermark).
///
/// # Errors
///
/// Same as [`stream_fetch`].
#[allow(clippy::too_many_arguments)]
pub async fn stream_fetch_shared(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    timeout: Duration,
    max_blob_size_bytes: u64,
) -> anyhow::Result<Bytes> {
    tokio::time::timeout(
        timeout,
        fetch_inner(
            endpoint,
            target,
            ctx,
            slash_domain,
            expected_signer,
            hash,
            byte_offset,
            timestamp_us,
            max_blob_size_bytes,
            ledger,
        ),
    )
    .await
    .map_err(|_| anyhow::Error::new(PullTimeout { after: timeout }))?
}

#[allow(clippy::too_many_arguments)]
async fn fetch_inner(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
    ledger: &ChannelLedger,
) -> anyhow::Result<Bytes> {
    // Full handshake — no 0-RTT on cdn/client/v1 (ADR 015).
    let conn = endpoint
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;

    let req = StreamRequest {
        hash,
        channel_id: ctx.channel_id.into(),
        byte_offset,
        timestamp_us,
    };
    // Two-phase encode (ADR 005): no ext for node-to-node pulls. The payload is
    // the `ClientMessage` plus any trailing ext bytes.
    let payload = decdn_protocol::encode_stream_request(&req, None)
        .map_err(|e| anyhow::anyhow!("encode stream request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write stream request: {e}"))?;

    let resp = match read_client_message(&mut recv).await? {
        ClientMessage::StreamResponse(r) => r,
        other => anyhow::bail!("expected StreamResponse, got {}", variant_name(&other)),
    };
    verify_response(
        &resp,
        slash_domain,
        expected_signer,
        hash,
        ctx.channel_id,
        timestamp_us,
    )?;

    if !resp.body.ok {
        anyhow::bail!("delivery refused: {:?}", resp.error);
    }
    if resp.body.redirect.is_some() {
        anyhow::bail!("server returned a redirect; following redirects is out of scope (#317)");
    }
    // Reject an oversized server-claimed `total_bytes` before allocating or
    // entering the receive loop — `total_bytes` is server-controlled and
    // `StreamResponse::validate()` does not bound it, so the in-loop
    // `cumulative > expected` guard alone would let one inflated promise drive
    // us toward OOM. Mirrors the serving-side `BlobTooLarge` gate
    // (handlers/client.rs); `0` = unlimited (#840). Typed sentinel so the pull
    // orchestrator classifies it as a buyer-side policy rejection, not provider
    // misbehavior.
    if max_blob_size_bytes > 0 && resp.body.total_bytes > max_blob_size_bytes {
        return Err(anyhow::Error::new(BlobTooLargeClaim {
            claimed: resp.body.total_bytes,
            ceiling: max_blob_size_bytes,
        }));
    }
    // A `total_bytes` below `byte_offset` would underflow `expected` to `0`
    // (saturating), so the loop ends on the first `StreamEnd` and returns an
    // empty buffer. On a resumed fetch (`byte_offset > 0`) the whole-blob hash
    // check is skipped, so that empty buffer would surface as success — a silent
    // verification bypass. A legitimate server always claims
    // `total_bytes >= byte_offset`; reject anything less before the loop. (A
    // non-empty but *short* delivery is caught by the completeness check after
    // the loop.)
    if resp.body.total_bytes < byte_offset {
        anyhow::bail!(
            "server claimed total_bytes ({}) below the requested byte_offset ({})",
            resp.body.total_bytes,
            byte_offset
        );
    }

    let rate_per_mb = resp.body.rate_per_mb;
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);
    let expected = resp.body.total_bytes.saturating_sub(byte_offset);

    let (buf, cumulative) = receive_and_pay(
        &mut send,
        &mut recv,
        ctx,
        ledger,
        rate_per_mb,
        interval_bytes,
        expected,
    )
    .await?;

    let blob = buf.freeze();
    // On a resumed fetch (`byte_offset > 0`) the whole-blob hash check below is
    // skipped, so a truncated delivery — fewer than `expected` bytes before
    // `StreamEnd` — would otherwise surface as a successful short read. The
    // server's `total_bytes` is the only completeness signal without the hash,
    // so require the full promised remainder. A full fetch (`byte_offset == 0`)
    // is covered by the hash check and may legitimately be shorter than an
    // over-claimed `total_bytes` as long as the bytes hash correctly, so this
    // is scoped to resumes only (#840).
    if byte_offset > 0 && cumulative < expected {
        conn.close(0u32.into(), b"short-delivery");
        anyhow::bail!("server sent {cumulative} of {expected} promised bytes before StreamEnd");
    }
    // Whole-blob integrity check on a full fetch (see module docs for the
    // resume caveat). `blake3::hash` is exactly what iroh-blobs content-
    // addresses with, so this is the same check the node performs.
    if byte_offset == 0 && blake3::hash(&blob) != blake3::Hash::from_bytes(hash) {
        conn.close(0u32.into(), b"hash-mismatch");
        // Typed sentinel (not a bare string) so callers can `downcast_ref` to
        // classify a paid-but-corrupt delivery; `Display` keeps the same text.
        return Err(anyhow::Error::new(HashMismatch));
    }
    conn.close(0u32.into(), b"done");
    Ok(blob)
}

/// Drive the buffered receive loop: read `ChunkData` into a buffer, paying one
/// voucher per `interval_bytes` boundary (and a closing voucher once all `expected`
/// bytes have arrived) through the shared `ledger`, until `StreamEnd`. Returns the
/// assembled buffer and the cumulative byte count for the caller's completeness +
/// hash checks. Enforces the chunk-size ceiling and the `cumulative <= expected`
/// overrun guard (ADR 005 §`cdn/client/v1`).
async fn receive_and_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    rate_per_mb: u64,
    interval_bytes: u64,
    expected: u64,
) -> anyhow::Result<(BytesMut, u64)> {
    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    // Bytes received but not yet covered by a voucher — the per-voucher *delta*
    // handed to the ledger (which accumulates deltas across this and any concurrent
    // streams on the channel, advancing only after the upstream acks).
    let mut bytes_since_voucher: u64 = 0;

    loop {
        match read_client_message(recv).await? {
            ClientMessage::ChunkData(chunk) => {
                // A chunk must not exceed the protocol ceiling, and the running
                // total must not exceed what the response promised — otherwise a
                // malicious server could stream unbounded bytes (OOM) and we
                // would overpay (ADR 005 §`cdn/client/v1`).
                if chunk.bytes.len() > decdn_protocol::CHUNK_SIZE {
                    anyhow::bail!("chunk of {} bytes exceeds CHUNK_SIZE", chunk.bytes.len());
                }
                cumulative = cumulative.saturating_add(chunk.bytes.len() as u64);
                if cumulative > expected {
                    anyhow::bail!(
                        "server sent {cumulative} bytes, more than the {expected} promised"
                    );
                }
                buf.extend_from_slice(&chunk.bytes);
                bytes_since_voucher = bytes_since_voucher.saturating_add(chunk.bytes.len() as u64);
                // Pay at each interval boundary, and a closing voucher once all
                // expected bytes have arrived — matching the node's pacing.
                let boundary = bytes_since_voucher >= interval_bytes && interval_bytes > 0;
                let closing = cumulative >= expected && bytes_since_voucher > 0;
                if boundary || closing {
                    self_pay(send, recv, ctx, ledger, rate_per_mb, bytes_since_voucher).await?;
                    bytes_since_voucher = 0;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other)),
        }
    }

    Ok((buf, cumulative))
}

/// Header fields from the upstream `StreamResponse`, surfaced by
/// [`open_progressive_pull`] before the first chunk so the fused serve path
/// (#856) knows `total_bytes` up front — it must sign its OWN downstream
/// `StreamResponse` (which commits to a `total_bytes`) before forwarding a byte.
#[derive(Debug, Clone, Copy)]
pub struct UpstreamPullHeader {
    /// Whole-blob size the upstream promised (echoed into our downstream
    /// `StreamResponse`).
    pub total_bytes: u64,
    /// Upstream rate; informational for the caller (the buyer pays it inside
    /// [`UpstreamPull::next_chunk`]).
    pub rate_per_mb: u64,
    /// Upstream voucher cadence in bytes (the buyer pays one voucher per
    /// interval as chunks arrive).
    pub interval_bytes: u64,
}

/// A live, progressive `cdn/client/v1` pull (#856), the streaming counterpart of
/// the buffered [`stream_fetch`]. Opened by [`open_progressive_pull`] (which has
/// already done the handshake and verified the response), driven chunk-by-chunk
/// via [`Self::next_chunk`], and closed by [`Self::finish`] (completeness +
/// whole-blob hash) or [`Self::abort`].
///
/// It pays the upstream per voucher interval *inside* `next_chunk` — identical
/// pacing to `stream_fetch` — but yields each chunk to the caller (which
/// forwards it to the paying downstream client and tees it into the cache)
/// instead of buffering the whole blob. This is what lets the serving node cap
/// its speculative exposure to a bounded window rather than fronting the entire
/// upstream cost before any downstream voucher arrives.
///
/// Unlike `stream_fetch_tracked`, the acked voucher watermark is OWNED here (not
/// threaded as a `&mut` out-param) and read back via [`Self::progress`] /
/// returned by `finish`/`abort` — the caller (`node_origin`) persists it. On any
/// exit, the caller MUST call `progress`/`finish`/`abort` to recover the
/// watermark for `record_progress` (#852); a [`Drop`] guard closes the
/// connection if none ran, but cannot return the watermark, so the obligation
/// stands.
///
/// **Deadlines.** The serve loop bounds only the *open* (handshake) phase with
/// the pull-through deadline. The streaming `next_chunk`/`finish` reads here are
/// NOT each deadline-bounded: a stalled upstream mid-stream is bounded by the
/// QUIC idle timeout and by the loop's own pacing — it stops pulling and recoups
/// a downstream voucher every window, so it cannot run unboundedly ahead of
/// (unpaid) downstream demand — rather than by a per-read timeout in this type.
pub struct UpstreamPull {
    conn: iroh::endpoint::Connection,
    send: SendStream,
    recv: RecvStream,
    ctx: ChannelContext,
    progress: VoucherProgress,
    hash: [u8; 32],
    byte_offset: u64,
    rate_per_mb: u64,
    interval_bytes: u64,
    /// Promised remaining bytes (`total_bytes - byte_offset`).
    expected: u64,
    /// Bytes received so far on this stream.
    cumulative: u64,
    /// Bytes received since the last voucher.
    unvouchered: u64,
    /// Incremental whole-blob BLAKE3 (only fed/checked for a full fetch,
    /// `byte_offset == 0`) — hashes chunks as they stream so we never buffer the
    /// blob just to verify it.
    hasher: blake3::Hasher,
    /// `StreamEnd` seen — `next_chunk` returns `None` and `finish` skips the
    /// drain.
    ended: bool,
}

impl std::fmt::Debug for UpstreamPull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamPull")
            .field("hash", &blake3::Hash::from_bytes(self.hash))
            .field("expected", &self.expected)
            .field("cumulative", &self.cumulative)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

/// Open a progressive `cdn/client/v1` pull (#856): connect, send the
/// [`StreamRequest`], read and verify the signed [`StreamResponse`] (so
/// `total_bytes` is known up front), and return its header plus a live
/// [`UpstreamPull`] to drive. The same response-validation rules as
/// [`stream_fetch`] apply — zero-rate rejection, `slash_sig` recovery, echoed
/// field checks, the [`BlobTooLargeClaim`] ceiling, and the
/// `total_bytes >= byte_offset` floor — all enforced BEFORE the first chunk.
///
/// # Errors
///
/// Same set as [`stream_fetch`] for the handshake/response phase (connect /
/// transport, refused or zero-rate response, bad `slash_sig`, mismatched echoed
/// field, oversized `total_bytes`).
#[allow(clippy::too_many_arguments)]
pub async fn open_progressive_pull(
    endpoint: &Endpoint,
    target: EndpointAddr,
    ctx: &ChannelContext,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    byte_offset: u64,
    timestamp_us: u64,
    max_blob_size_bytes: u64,
) -> anyhow::Result<(UpstreamPullHeader, UpstreamPull)> {
    // Full handshake — no 0-RTT on cdn/client/v1 (ADR 015).
    let conn = endpoint
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;

    let req = StreamRequest {
        hash,
        channel_id: ctx.channel_id.into(),
        byte_offset,
        timestamp_us,
    };
    let payload = decdn_protocol::encode_stream_request(&req, None)
        .map_err(|e| anyhow::anyhow!("encode stream request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write stream request: {e}"))?;

    let resp = match read_client_message(&mut recv).await? {
        ClientMessage::StreamResponse(r) => r,
        other => anyhow::bail!("expected StreamResponse, got {}", variant_name(&other)),
    };
    verify_response(
        &resp,
        slash_domain,
        expected_signer,
        hash,
        ctx.channel_id,
        timestamp_us,
    )?;
    if !resp.body.ok {
        anyhow::bail!("delivery refused: {:?}", resp.error);
    }
    if resp.body.redirect.is_some() {
        anyhow::bail!("server returned a redirect; following redirects is out of scope (#317)");
    }
    // Same buyer-side ceiling as `fetch_inner`: reject an inflated `total_bytes`
    // before forwarding/allocating anything (#840). Typed sentinel so the pull
    // orchestrator classifies it as a buyer policy rejection, not provider fault.
    if max_blob_size_bytes > 0 && resp.body.total_bytes > max_blob_size_bytes {
        return Err(anyhow::Error::new(BlobTooLargeClaim {
            claimed: resp.body.total_bytes,
            ceiling: max_blob_size_bytes,
        }));
    }
    if resp.body.total_bytes < byte_offset {
        anyhow::bail!(
            "server claimed total_bytes ({}) below the requested byte_offset ({})",
            resp.body.total_bytes,
            byte_offset
        );
    }

    let rate_per_mb = resp.body.rate_per_mb;
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);
    let expected = resp.body.total_bytes.saturating_sub(byte_offset);
    let header = UpstreamPullHeader {
        total_bytes: resp.body.total_bytes,
        rate_per_mb,
        interval_bytes,
    };
    let pull = UpstreamPull {
        conn,
        send,
        recv,
        ctx: ctx.clone(),
        progress: VoucherProgress::seed(ctx),
        hash,
        byte_offset,
        rate_per_mb,
        interval_bytes,
        expected,
        cumulative: 0,
        unvouchered: 0,
        hasher: blake3::Hasher::new(),
        ended: false,
    };
    Ok((header, pull))
}

impl UpstreamPull {
    /// Bytes this stream will deliver: the upstream's promised `total_bytes`
    /// minus the requested `byte_offset` (the full `total_bytes` for a fresh
    /// fetch, the remaining suffix for a resumed one).
    #[must_use]
    pub const fn expected(&self) -> u64 {
        self.expected
    }

    /// The current acked voucher watermark — read it on any exit (including an
    /// error from `next_chunk`) to persist what was paid (#852).
    #[must_use]
    pub const fn progress(&self) -> VoucherProgress {
        self.progress
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher
    /// through a one-shot ledger seeded from the current acked watermark, then copy
    /// the committed cumulative back into `self.progress`. The progressive pull owns
    /// a single stream, so there is no cross-stream contention to serialize here;
    /// the one-shot ledger just reuses the shared issue → sign → ack → commit path
    /// so the wire behavior matches `fetch_inner` exactly.
    async fn pay_one(&mut self, delta_bytes: u64) -> anyhow::Result<()> {
        let ledger = ChannelLedger::new(Cumulative {
            nonce: self.progress.nonce,
            bytes: self.progress.bytes_delivered,
            amount: self.progress.amount,
        });
        let result = self_pay(
            &mut self.send,
            &mut self.recv,
            &self.ctx,
            &ledger,
            self.rate_per_mb,
            delta_bytes,
        )
        .await;
        // Copy back the committed watermark on every path: on Ok the ledger
        // advanced; on a rejected voucher it stayed put, so `progress` is unchanged.
        self.progress
            .set_from_cumulative(ledger.snapshot().await, self.ctx.prior_nonce);
        result
    }

    /// Read the next `ChunkData`, paying the upstream at each voucher-interval
    /// boundary (and a closing voucher once all promised bytes have arrived),
    /// and return the chunk for the caller to forward downstream + tee to cache.
    /// Returns `Ok(None)` on `StreamEnd`.
    ///
    /// # Errors
    ///
    /// An over-`CHUNK_SIZE` chunk, more bytes than promised, a mid-stream
    /// `StreamError`, an unexpected message, or a [`UpstreamVoucherRejected`] /
    /// transport error while paying.
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        if self.ended {
            return Ok(None);
        }
        match read_client_message(&mut self.recv).await? {
            ClientMessage::ChunkData(chunk) => {
                if chunk.bytes.len() > decdn_protocol::CHUNK_SIZE {
                    anyhow::bail!("chunk of {} bytes exceeds CHUNK_SIZE", chunk.bytes.len());
                }
                self.cumulative = self.cumulative.saturating_add(chunk.bytes.len() as u64);
                if self.cumulative > self.expected {
                    anyhow::bail!(
                        "server sent {} bytes, more than the {} promised",
                        self.cumulative,
                        self.expected
                    );
                }
                // Feed the incremental hash only for a full fetch — a resumed
                // fetch cannot recompute the whole-blob hash from a suffix.
                if self.byte_offset == 0 {
                    self.hasher.update(&chunk.bytes);
                }
                self.unvouchered = self.unvouchered.saturating_add(chunk.bytes.len() as u64);
                let boundary = self.unvouchered >= self.interval_bytes && self.interval_bytes > 0;
                let closing = self.cumulative >= self.expected && self.unvouchered > 0;
                if boundary || closing {
                    let delta = self.unvouchered;
                    self.pay_one(delta).await?;
                    self.unvouchered = 0;
                }
                Ok(Some(Bytes::from(chunk.bytes)))
            }
            ClientMessage::StreamEnd => {
                self.ended = true;
                Ok(None)
            }
            ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other)),
        }
    }

    /// Finalize a completed pull: drain to `StreamEnd` if needed, enforce the
    /// completeness check (resumes) and whole-blob hash (full fetches), close the
    /// connection cleanly, and return the final acked watermark to persist.
    ///
    /// # Errors
    ///
    /// [`HashMismatch`] on a corrupt full-fetch delivery, or a short/over-long
    /// delivery, mirroring [`stream_fetch`]'s completeness rules.
    pub async fn finish(mut self) -> anyhow::Result<VoucherProgress> {
        while !self.ended {
            match read_client_message(&mut self.recv).await? {
                ClientMessage::StreamEnd => self.ended = true,
                ClientMessage::ChunkData(_) => {
                    anyhow::bail!("server sent ChunkData after the promised total")
                }
                ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
                other => {
                    anyhow::bail!("unexpected message at stream end: {}", variant_name(&other))
                }
            }
        }
        // Resume completeness (see `fetch_inner` for the rationale): a resumed
        // fetch has no whole-blob hash, so the promised remainder is the only
        // completeness signal.
        if self.byte_offset > 0 && self.cumulative < self.expected {
            self.conn.close(0u32.into(), b"short-delivery");
            anyhow::bail!(
                "server sent {} of {} promised bytes before StreamEnd",
                self.cumulative,
                self.expected
            );
        }
        // Whole-blob integrity on a full fetch — the incremental hash over the
        // forwarded chunks must equal the requested content hash.
        if self.byte_offset == 0 {
            let digest = self.hasher.finalize();
            if digest != blake3::Hash::from_bytes(self.hash) {
                self.conn.close(0u32.into(), b"hash-mismatch");
                return Err(anyhow::Error::new(HashMismatch));
            }
        }
        self.conn.close(0u32.into(), b"done");
        Ok(self.progress)
    }

    /// Abandon the pull (e.g. the downstream client dropped, so we stop pulling
    /// and paying). Closes the connection and returns the acked watermark so the
    /// caller can still persist what it paid (#852).
    #[must_use]
    pub fn abort(self) -> VoucherProgress {
        self.conn.close(0u32.into(), b"client-abandoned");
        self.progress
    }
}

impl Drop for UpstreamPull {
    /// Safety net for the "call a terminal method on every exit" contract: if a
    /// caller returns or panics without `finish`/`abort`, still close the upstream
    /// connection so the QUIC stream and the upstream's server-side serve task
    /// don't linger and keep that paid stream half-open. `Connection::close` is
    /// first-wins and idempotent, so an explicit close in `finish`/`abort` keeps
    /// its richer reason and this is a no-op when one of them ran; it only takes
    /// effect on a dropped-without-finalize path. The acked watermark cannot be
    /// recovered from `drop` (it can't be returned), so this bounds only the
    /// connection leak, not the #852 watermark loss the doc contract guards.
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"upstream-pull-dropped");
    }
}

/// Issue one cumulative voucher for `delta_bytes` newly delivered since the last
/// voucher, then await `VoucherAck`.
///
/// Voucher issuance runs through the channel's [`ChannelLedger`], which serializes
/// the compute → sign → send → await-ack → commit cycle across every concurrent
/// stream on the channel: the ledger holds its lock across the whole exchange, so
/// vouchers reach the node in strict nonce order even while byte transfers run in
/// parallel. Each voucher's own *delta* (`ceil(delta_bytes * rate / 1 MiB)`)
/// covers its own bytes at the advertised rate (the node checks deltas, not the
/// rounded cumulative). The ledger commits the advanced cumulative **only after**
/// the upstream acks, so a rejected voucher leaves the watermark at the last acked
/// value.
async fn self_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    ledger: &ChannelLedger,
    rate_per_mb: u64,
    delta_bytes: u64,
) -> anyhow::Result<()> {
    ledger
        .issue(delta_bytes, rate_per_mb, |next: Cumulative| async move {
            let signed = Voucher {
                channel_id: ctx.channel_id,
                amount: next.amount,
                nonce: next.nonce,
                bytes_delivered: next.bytes,
                token: ctx.token,
            }
            .sign(ctx.client_signer.as_ref(), &ctx.voucher_domain)
            .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
            write_message(
                send,
                &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
            )
            .await?;
            match read_client_message(recv).await? {
                // The upstream persists before it acks (ADR 003), so this is the
                // cumulative total it has accepted — let the ledger commit it.
                ClientMessage::VoucherAck => Ok(()),
                // Only a `VoucherRejected` is OUR payment-side fault. Carry its
                // typed reason so the orchestrator can exonerate the provider
                // (#857). Any OTHER `StreamError` here is the upstream violating
                // the ack protocol (only `VoucherAck`/`VoucherRejected` are valid
                // in reply to a voucher), so it stays a bare error and is
                // classified as provider-attributable upstream.
                ClientMessage::StreamError(StreamError::VoucherRejected { reason }) => {
                    Err(anyhow::Error::new(UpstreamVoucherRejected { reason }))
                }
                ClientMessage::StreamError(e) => {
                    anyhow::bail!("unexpected stream error awaiting voucher ack: {e:?}")
                }
                other => anyhow::bail!("expected VoucherAck, got {}", variant_name(&other)),
            }
        })
        .await
        .map(|_committed| ())
}

/// Validate + verify a `StreamResponse` on receive (ADR 005, ADR 014 §1, #252).
fn verify_response(
    resp: &StreamResponse,
    slash_domain: &Eip712Domain,
    expected_signer: Address,
    hash: [u8; 32],
    channel_id: B256,
    timestamp_us: u64,
) -> anyhow::Result<()> {
    // #252 + slash_sig length/upper-bound checks.
    resp.validate()
        .map_err(|e| anyhow::anyhow!("invalid stream response: {e}"))?;
    // Echoed-field correlation (ADR 005).
    if resp.body.hash != hash {
        anyhow::bail!("response hash does not match request");
    }
    if resp.body.channel_id != channel_id.as_slice() {
        anyhow::bail!("response channel_id does not match request");
    }
    if resp.body.timestamp_us != timestamp_us {
        anyhow::bail!("response timestamp_us not echoed");
    }
    // slash_sig must recover to the delivering node's Ethereum address.
    let sig = Signature::try_from(resp.slash_sig.as_slice())
        .map_err(|e| anyhow::anyhow!("slash_sig parse: {e}"))?;
    StreamSlashData::from_response_body(&resp.body)
        .verify_signer(&sig, expected_signer, slash_domain)
        .map_err(|e| anyhow::anyhow!("slash_sig verification failed: {e}"))?;
    Ok(())
}

async fn write_message(send: &mut SendStream, msg: &ClientMessage) -> anyhow::Result<()> {
    let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("encode failed: {e}"))?;
    write_frame(send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write failed: {e}"))
}

async fn read_client_message(recv: &mut RecvStream) -> anyhow::Result<ClientMessage> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("frame read failed: {e}"))?;
    let (msg, _rest) = decode_message::<ClientMessage>(&frame)
        .map_err(|e| anyhow::anyhow!("decode failed: {e}"))?;
    Ok(msg)
}

const fn variant_name(msg: &ClientMessage) -> &'static str {
    match msg {
        ClientMessage::StreamRequest(_) => "StreamRequest",
        ClientMessage::StreamResponse(_) => "StreamResponse",
        ClientMessage::ChunkData(_) => "ChunkData",
        ClientMessage::Voucher(_) => "Voucher",
        ClientMessage::VoucherAck => "VoucherAck",
        ClientMessage::StreamEnd => "StreamEnd",
        ClientMessage::StreamError(_) => "StreamError",
        ClientMessage::CooperativeCloseRequest(_) => "CooperativeCloseRequest",
        ClientMessage::CooperativeCloseAuth(_) => "CooperativeCloseAuth",
    }
}

#[cfg(test)]
mod tests {
    /// The progressive pull verifies integrity with an INCREMENTAL BLAKE3 over
    /// the forwarded chunks (`UpstreamPull::finish`) instead of `blake3::hash`
    /// over a buffered blob (`fetch_inner`). This pins the equivalence the swap
    /// relies on: feeding a hasher chunk-by-chunk yields the same content hash
    /// iroh-blobs addresses with, for any chunk split.
    #[test]
    fn incremental_blake3_matches_whole_blob_hash() {
        let payload: Vec<u8> = (0..300_000u32)
            .map(|i| u8::try_from(i % 256).unwrap_or(0))
            .collect();
        let whole = blake3::hash(&payload);

        for chunk_len in [1usize, 7, 1024, 65_536, payload.len()] {
            let mut hasher = blake3::Hasher::new();
            for chunk in payload.chunks(chunk_len) {
                hasher.update(chunk);
            }
            let incremental = hasher.finalize();
            assert_eq!(
                incremental, whole,
                "incremental hash with chunk_len={chunk_len} must equal blake3::hash"
            );
        }
    }
}
