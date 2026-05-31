//! Reusable `cdn/client/v1` requester for node-to-node paid pulls (#317).
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

use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::local::PrivateKeySigner;
use bytes::{Bytes, BytesMut};
use decdn_cache::Hash;
use decdn_incentive::{StreamSlashData, Voucher, signed_to_wire_voucher};
use decdn_protocol::client::{ClientMessage, StreamRequest, StreamResponse};
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
    /// `StablePaymentChannel` EIP-712 domain.
    pub voucher_domain: Eip712Domain,
    /// Nonce of the last voucher the client issued on this channel (the next
    /// voucher uses `prior_nonce + 1`). `ZERO` for a fresh channel.
    pub prior_nonce: U256,
    /// Cumulative bytes paid for on this channel before this stream.
    pub prior_bytes_delivered: U256,
    /// Cumulative amount paid on this channel before this stream.
    pub prior_amount: U256,
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

/// Fetch `hash` from `target` over `cdn/client/v1`, paying as bytes arrive.
///
/// `expected_signer` is the delivering node's Ethereum address, used to verify
/// the response `slash_sig`. `byte_offset` resumes a partial fetch.
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
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("stream_fetch timed out after {timeout:?}"))??;
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

    let rate_per_mb = resp.body.rate_per_mb;
    let interval_bytes = resp
        .voucher_interval_mb
        .unwrap_or(DEFAULT_VOUCHER_INTERVAL_MB)
        .saturating_mul(MB_BYTES);
    let expected = resp.body.total_bytes.saturating_sub(byte_offset);

    let mut buf = BytesMut::new();
    let mut cumulative: u64 = 0;
    let mut unvouchered: u64 = 0;
    let mut vouchers_sent: u64 = 0;
    // Running cumulative voucher state, seeded from the channel's prior state so
    // each voucher's *delta* (not the rounded cumulative) covers its own bytes —
    // otherwise two small streams that round to the same cumulative amount
    // produce a zero-delta voucher the node rejects as underpayment.
    let mut paid_bytes = ctx.prior_bytes_delivered;
    let mut paid_amount = ctx.prior_amount;

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
                    self_pay(
                        &mut send,
                        &mut recv,
                        ctx,
                        rate_per_mb,
                        cumulative,
                        &mut paid_bytes,
                        &mut paid_amount,
                        &mut vouchers_sent,
                    )
                    .await?;
                    unvouchered = 0;
                }
            }
            ClientMessage::StreamEnd => break,
            ClientMessage::StreamError(e) => anyhow::bail!("stream failed: {e:?}"),
            other => anyhow::bail!("unexpected message mid-delivery: {}", variant_name(&other)),
        }
    }

    let blob = buf.freeze();
    // Whole-blob integrity check on a full fetch (see module docs for the
    // resume caveat).
    if byte_offset == 0 && Hash::new(&blob) != Hash::from_bytes(hash) {
        conn.close(0u32.into(), b"hash-mismatch");
        anyhow::bail!("received bytes do not match requested hash");
    }
    conn.close(0u32.into(), b"done");
    Ok(blob)
}

/// Sign and send a cumulative voucher for the channel-wide bytes delivered so
/// far (prior state + `stream_bytes` of this stream), then await `VoucherAck`.
///
/// Vouchers are cumulative across the channel's lifetime. The amount is built up
/// from `paid_amount` by adding `ceil(bytes_delta * rate / 1 MiB)` for the bytes
/// since the previous voucher, so every voucher's *delta* covers its own bytes
/// at the advertised rate (the node checks deltas, not the rounded cumulative).
/// Seeding `paid_*`/nonce from [`ChannelContext`] `prior_*` is what lets a reused
/// channel resume rather than regress.
#[allow(clippy::too_many_arguments)]
async fn self_pay(
    send: &mut SendStream,
    recv: &mut RecvStream,
    ctx: &ChannelContext,
    rate_per_mb: u64,
    stream_bytes: u64,
    paid_bytes: &mut U256,
    paid_amount: &mut U256,
    vouchers_sent: &mut u64,
) -> anyhow::Result<()> {
    *vouchers_sent = vouchers_sent.saturating_add(1);
    let nonce = ctx.prior_nonce.saturating_add(U256::from(*vouchers_sent));
    let new_bytes = ctx
        .prior_bytes_delivered
        .saturating_add(U256::from(stream_bytes));
    let bytes_delta = new_bytes.saturating_sub(*paid_bytes);
    let amount_delta = bytes_delta
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES));
    let amount = paid_amount.saturating_add(amount_delta);

    let signed = Voucher {
        channel_id: ctx.channel_id,
        amount,
        nonce,
        bytes_delivered: new_bytes,
        token: ctx.token,
    }
    .sign(ctx.client_signer.as_ref(), &ctx.voucher_domain)
    .map_err(|e| anyhow::anyhow!("voucher signing failed: {e}"))?;
    *paid_bytes = new_bytes;
    *paid_amount = amount;

    write_message(
        send,
        &ClientMessage::Voucher(signed_to_wire_voucher(&signed)),
    )
    .await?;
    match read_client_message(recv).await? {
        ClientMessage::VoucherAck => Ok(()),
        ClientMessage::StreamError(e) => anyhow::bail!("voucher rejected: {e:?}"),
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
    }
}
