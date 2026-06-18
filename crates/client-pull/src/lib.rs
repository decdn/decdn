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
/// [`stream_fetch_tracked`] seeds the watermark from the channel's prior
/// cumulative state (`seed`) and advances it after **each** `VoucherAck`
/// (`commit_ack`) — so it always holds the
/// **absolute** cumulative totals of the last *acked* voucher (not per-stream
/// deltas), exactly the triple `BuyerChannelService::record_progress` expects.
/// Because the update is in place after the ack, the latest acked totals survive
/// an `Err` return or a timeout cancellation, so a mid-stream failure or a
/// paid-but-corrupt delivery is still recorded against the upstream's committed
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

    /// Advance the watermark to an acked voucher's absolute cumulative totals.
    /// All four fields move together; call this **only** once the upstream has
    /// acked, so a signed-but-rejected voucher never moves the watermark.
    const fn commit_ack(&mut self, nonce: U256, bytes_delivered: U256, amount: U256) {
        self.nonce = nonce;
        self.bytes_delivered = bytes_delivered;
        self.amount = amount;
        self.vouchers_sent = self.vouchers_sent.saturating_add(1);
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
/// `progress` is updated in place with the cumulative `(nonce, bytes_delivered,
/// amount)` of each acked voucher; it is threaded into `fetch_inner` by reference
/// (not captured by value) so the writes made before a timeout cancellation
/// persist. On return — `Ok`, `Err`, or timeout — it holds the last acked totals,
/// so the caller can record progress even for a mid-stream failure or a
/// paid-but-corrupt delivery. See [`VoucherProgress`].
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
    let bytes = tokio::time::timeout(
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
            progress,
        ),
    )
    .await
    .map_err(|_| anyhow::Error::new(PullTimeout { after: timeout }))??;
    Ok(bytes)
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
    progress: &mut VoucherProgress,
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

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    // Seed the acked watermark from the channel's prior cumulative state (the last
    // voucher acked on earlier streams). Each `self_pay` advances it after the
    // upstream acks, so each voucher's *delta* (not the rounded cumulative) covers
    // its own bytes — otherwise two small streams that round to the same
    // cumulative amount produce a zero-delta voucher the node rejects as
    // underpayment.
    *progress = VoucherProgress::seed(ctx);

    loop {
        match read_client_message(&mut recv).await? {
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
                unvouchered = unvouchered.saturating_add(chunk.bytes.len() as u64);
                // Pay at each interval boundary, and a closing voucher once all
                // expected bytes have arrived — matching the node's pacing.
                let boundary = unvouchered >= interval_bytes && interval_bytes > 0;
                let closing = cumulative >= expected && unvouchered > 0;
                if boundary || closing {
                    self_pay(&mut send, &mut recv, ctx, rate_per_mb, cumulative, progress).await?;
                    unvouchered = 0;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other)),
        }
    }

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
                    self_pay(
                        &mut self.send,
                        &mut self.recv,
                        &self.ctx,
                        self.rate_per_mb,
                        self.cumulative,
                        &mut self.progress,
                    )
                    .await?;
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

/// Sign and send a cumulative voucher for the channel-wide bytes delivered so
/// far (prior state + `stream_bytes` of this stream), then await `VoucherAck`.
///
/// Vouchers are cumulative across the channel's lifetime. The amount is built up
/// from `progress.amount` by adding `ceil(bytes_delta * rate / 1 MiB)` for the
/// bytes since the previous voucher, so every voucher's *delta* covers its own
/// bytes at the advertised rate (the node checks deltas, not the rounded
/// cumulative). The watermark in `progress` (seeded from [`ChannelContext`]
/// `prior_*`) is what lets a reused channel resume rather than regress. It
/// advances via [`VoucherProgress::commit_ack`] **only after** the upstream acks,
/// so a rejected voucher leaves the persisted watermark at the last acked value.
async fn self_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    rate_per_mb: u64,
    stream_bytes: u64,
    progress: &mut VoucherProgress,
) -> anyhow::Result<()> {
    let next_count = progress.vouchers_sent.saturating_add(1);
    let nonce = ctx.prior_nonce.saturating_add(U256::from(next_count));
    let new_bytes = ctx
        .prior_bytes_delivered
        .saturating_add(U256::from(stream_bytes));
    let bytes_delta = new_bytes.saturating_sub(progress.bytes_delivered);
    let amount_delta = bytes_delta
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES));
    let amount = progress.amount.saturating_add(amount_delta);

    let signed = Voucher {
        channel_id: ctx.channel_id,
        amount,
        nonce,
        bytes_delivered: new_bytes,
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
        // Commit the acked watermark only now: the upstream persists before it
        // acks (ADR 003), so this is the cumulative total it has accepted.
        ClientMessage::VoucherAck => {
            progress.commit_ack(nonce, new_bytes, amount);
            Ok(())
        }
        // Only a `VoucherRejected` is OUR payment-side fault. Carry its typed
        // reason so the orchestrator can exonerate the provider (#857). Any OTHER
        // `StreamError` here is the upstream violating the ack protocol (only
        // `VoucherAck`/`VoucherRejected` are valid in reply to a voucher), so it
        // stays a bare error and is classified as provider-attributable upstream.
        ClientMessage::StreamError(StreamError::VoucherRejected { reason }) => {
            Err(anyhow::Error::new(UpstreamVoucherRejected { reason }))
        }
        ClientMessage::StreamError(e) => {
            anyhow::bail!("unexpected stream error awaiting voucher ack: {e:?}")
        }
        other => anyhow::bail!("expected VoucherAck, got {}", variant_name(&other)),
    }
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
