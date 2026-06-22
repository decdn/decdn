//! Client-initiated cooperative close (#971).
//!
//! The provider half — sign a `CooperativeClose` waiver on request, then stop
//! serving — shipped with the node handler (#957). This is the buyer/initiator
//! half, shared by the `decdn` CLI (`channel coop-close`) and, later, the node's
//! node→node buyer reconcile (#972). One settle, no dispute window (ADR 003
//! §Cooperative close):
//!
//! 1. Ask the provider for a waiver over the channel's final watermark
//!    (`request_cooperative_close_auth`).
//! 2. Refuse if the provider's returned tuple exceeds what the client actually
//!    authorized, then verify the waiver signature really is the provider's.
//! 3. Sign a client voucher over the agreed tuple and submit `cooperativeClose`
//!    in one tx (`cooperative_close`).
//!
//! If the provider has no channel or no voucher yet it finishes the stream with
//! no auth (`CooperativeCloseOutcome::Declined`); the caller falls back to the
//! ordinary `closeChannel` → dispute-window → `settleChannel` path.

use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, Signature, U256};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{CooperativeClose, SignedCooperativeClose, Voucher};
use decdn_protocol::client::{ClientMessage, CooperativeCloseAuth, CooperativeCloseRequest};
use decdn_protocol::{
    ALPN_CLIENT, FrameError, decode_message, encode_message, read_frame, write_frame,
};
use iroh::{Endpoint, EndpointAddr};

/// What the client authorized on the channel — the cap the provider's returned
/// settlement tuple may not exceed.
///
/// The on-chain `cooperativeClose` already bounds `amount` by the deposit and by
/// the monotonic on-chain `claimed*` watermark, but neither stops a provider
/// from asking the client to *sign away* more than it actually owes (up to the
/// full deposit). This cap is the off-chain guard: the client refuses to sign a
/// voucher above what it issued. For a buyer-held channel these come straight
/// from the persisted `BuyerChannelState` (`last_amount`/`last_nonce`/
/// `last_bytes_delivered`).
#[derive(Debug, Clone, Copy)]
pub struct AuthorizedWatermark {
    /// Highest cumulative amount the client has signed a voucher for.
    pub amount: U256,
    /// Highest voucher nonce the client has issued.
    pub nonce: U256,
    /// Highest cumulative bytes the client has paid for.
    pub bytes_delivered: U256,
}

/// Result of a client-initiated cooperative close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CooperativeCloseOutcome {
    /// Settled on-chain in one tx; the channel is terminal `Closed`.
    Settled,
    /// The provider has no channel or no accepted voucher and finished without a
    /// waiver. The caller should fall back to `closeChannel`.
    Declined,
    /// The `cooperativeClose` tx was mined but reverted (e.g. it raced a
    /// `withdraw`/`closeChannel`, or the watermark regressed against the on-chain
    /// `claimed*`). The caller should fall back to `closeChannel`.
    Reverted,
}

/// Ask `target` for a cooperative-close waiver on `channel_id` over
/// `cdn/client/v1`.
///
/// Returns `Ok(Some(auth))` with the provider's waiver, or `Ok(None)` when the
/// provider finishes the stream without one — its decline signal for an unknown
/// channel or a channel with no accepted voucher yet (handlers/client.rs). A
/// network/protocol failure is `Err`.
///
/// # Errors
///
/// Dial/stream failures, a non-auth reply, a malformed auth frame, or timeout.
pub async fn request_cooperative_close_auth(
    endpoint: &Endpoint,
    target: EndpointAddr,
    channel_id: B256,
    timeout: Duration,
) -> anyhow::Result<Option<CooperativeCloseAuth>> {
    tokio::time::timeout(timeout, request_inner(endpoint, target, channel_id))
        .await
        .map_err(|_| anyhow::anyhow!("cooperative-close request timed out after {timeout:?}"))?
}

async fn request_inner(
    endpoint: &Endpoint,
    target: EndpointAddr,
    channel_id: B256,
) -> anyhow::Result<Option<CooperativeCloseAuth>> {
    // Full handshake — no 0-RTT on cdn/client/v1 (ADR 015), mirroring the pull path.
    let conn = endpoint
        .connect(target, ALPN_CLIENT)
        .await
        .map_err(|e| anyhow::anyhow!("connect failed: {e}"))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| anyhow::anyhow!("open_bi failed: {e}"))?;

    let req = ClientMessage::CooperativeCloseRequest(CooperativeCloseRequest {
        channel_id: channel_id.into(),
    });
    let payload = encode_message(&req).map_err(|e| anyhow::anyhow!("encode request: {e}"))?;
    write_frame(&mut send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write request: {e}"))?;
    send.finish()
        .map_err(|e| anyhow::anyhow!("finish send stream: {e}"))?;

    // A clean stream end with no frame is the provider's decline: the handler
    // calls `send.finish()` without a reply for an unknown channel or a
    // zero-voucher channel. That surfaces here as an `UnexpectedEof` while
    // reading the length prefix — map it to `None`; surface anything else.
    let frame = match read_frame(&mut recv).await {
        Ok(f) => f,
        Err(FrameError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None);
        }
        Err(e) => return Err(anyhow::anyhow!("read auth: {e}")),
    };
    let (msg, _rest) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode auth: {e}"))?;
    match msg {
        ClientMessage::CooperativeCloseAuth(auth) => {
            auth.validate()
                .map_err(|e| anyhow::anyhow!("invalid cooperative-close auth: {e}"))?;
            Ok(Some(auth))
        }
        other => anyhow::bail!("expected CooperativeCloseAuth, got {other:?}"),
    }
}

/// The two signatures + agreed tuple ready to submit to `cooperativeClose`.
#[derive(Debug, Clone)]
struct PreparedClose {
    amount: U256,
    nonce: U256,
    bytes_delivered: U256,
    /// Client voucher signature (`r‖s‖v`, `v` in 27/28) over the agreed tuple.
    client_sig: Bytes,
    /// Provider `CooperativeClose` waiver signature, echoed from the auth.
    provider_sig: Bytes,
}

/// Validate the provider's waiver against what the client authorized, then sign
/// the matching client voucher — the security-critical, network-free core.
///
/// Refuses if the provider's tuple exceeds the authorized watermark (a provider
/// trying to inflate the payout toward the full deposit) or if the waiver does
/// not recover to `provider_eth`.
///
/// # Errors
///
/// - provider over-claimed beyond `authorized`,
/// - the waiver signature is malformed or signed by the wrong key,
/// - client voucher signing fails.
fn prepare_close(
    auth: &CooperativeCloseAuth,
    channel_id: B256,
    provider_eth: Address,
    token: Address,
    authorized: AuthorizedWatermark,
    client_signer: &PrivateKeySigner,
    domain: &Eip712Domain,
) -> anyhow::Result<PreparedClose> {
    let amount = U256::from_be_bytes(auth.amount);
    let nonce = U256::from_be_bytes(auth.nonce);
    let bytes_delivered = U256::from_be_bytes(auth.bytes_delivered);

    // Never sign away more than we authorized. The provider returns the highest
    // client voucher it holds, which can only be <= what we issued; a larger
    // tuple means a misbehaving provider. Refuse — the channel can still be
    // closed the slow way.
    if amount > authorized.amount
        || nonce > authorized.nonce
        || bytes_delivered > authorized.bytes_delivered
    {
        anyhow::bail!(
            "provider over-claimed: waiver ({amount}, {nonce}, {bytes_delivered}) exceeds \
             authorized ({}, {}, {})",
            authorized.amount,
            authorized.nonce,
            authorized.bytes_delivered
        );
    }

    // Verify the waiver really is the provider's before spending gas.
    let provider_sig = Signature::try_from(auth.signature.as_slice())
        .map_err(|e| anyhow::anyhow!("malformed provider waiver signature: {e}"))?;
    SignedCooperativeClose {
        close: CooperativeClose {
            channel_id,
            amount,
            nonce,
            bytes_delivered,
            token,
        },
        signature: provider_sig,
    }
    .verify_signer(provider_eth, domain)
    .map_err(|e| anyhow::anyhow!("provider waiver verification failed: {e}"))?;

    // Sign our own voucher over the agreed tuple — the client sig the contract
    // checks against `channel.client`.
    let client_sig = Voucher {
        channel_id,
        amount,
        nonce,
        bytes_delivered,
        token,
    }
    .sign(client_signer, domain)
    .map_err(|e| anyhow::anyhow!("client voucher signing failed: {e}"))?
    .signature;

    Ok(PreparedClose {
        amount,
        nonce,
        bytes_delivered,
        // `Signature::as_bytes` already emits `v` in the 27/28 convention the
        // contract's `ECDSA.recover` expects (see node `normalize_voucher_signature`).
        client_sig: Bytes::from(client_sig.as_bytes().to_vec()),
        provider_sig: Bytes::from(auth.signature.clone()),
    })
}

/// Run a full client-initiated cooperative close against `target` and submit it
/// on-chain through `contract`.
///
/// Requests the waiver, validates + signs it against `authorized`, and submits
/// `cooperativeClose`. The contract enforces the deposit ceiling and monotonic
/// `claimed*` floor, so a revert is non-fatal ([`CooperativeCloseOutcome`]) — the
/// caller falls back to `closeChannel`.
///
/// # Errors
///
/// Network/protocol failure, provider over-claim, waiver verification failure,
/// signing failure, or an on-chain send/receipt error.
#[allow(clippy::too_many_arguments)]
pub async fn cooperative_close<P: Provider>(
    endpoint: &Endpoint,
    target: EndpointAddr,
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    channel_id: B256,
    provider_eth: Address,
    token: Address,
    authorized: AuthorizedWatermark,
    client_signer: &PrivateKeySigner,
    domain: &Eip712Domain,
    timeout: Duration,
) -> anyhow::Result<CooperativeCloseOutcome> {
    let Some(auth) = request_cooperative_close_auth(endpoint, target, channel_id, timeout).await?
    else {
        return Ok(CooperativeCloseOutcome::Declined);
    };

    let prepared = prepare_close(
        &auth,
        channel_id,
        provider_eth,
        token,
        authorized,
        client_signer,
        domain,
    )?;

    let pending = contract
        .cooperativeClose(
            channel_id,
            prepared.amount,
            prepared.nonce,
            prepared.bytes_delivered,
            prepared.client_sig,
            prepared.provider_sig,
        )
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("cooperativeClose send failed: {e}"))?;
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("cooperativeClose receipt failed: {e}"))?;
    if receipt.status() {
        Ok(CooperativeCloseOutcome::Settled)
    } else {
        Ok(CooperativeCloseOutcome::Reverted)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use decdn_incentive::voucher_domain;

    const CHAIN_ID: u64 = 421_614;

    fn domain() -> Eip712Domain {
        voucher_domain(CHAIN_ID, Address::repeat_byte(0xCC))
    }

    /// Build the auth a well-behaved provider would return for `(amount, nonce,
    /// bytes)`, signed by `provider`.
    fn provider_auth(
        provider: &PrivateKeySigner,
        channel_id: B256,
        token: Address,
        amount: U256,
        nonce: U256,
        bytes_delivered: U256,
    ) -> CooperativeCloseAuth {
        let signed = CooperativeClose {
            channel_id,
            amount,
            nonce,
            bytes_delivered,
            token,
        }
        .sign(provider, &domain())
        .expect("provider waiver signs");
        CooperativeCloseAuth {
            channel_id: channel_id.into(),
            amount: amount.to_be_bytes(),
            nonce: nonce.to_be_bytes(),
            bytes_delivered: bytes_delivered.to_be_bytes(),
            signature: signed.signature.as_bytes().to_vec(),
        }
    }

    fn watermark(amount: u64, nonce: u64, bytes: u64) -> AuthorizedWatermark {
        AuthorizedWatermark {
            amount: U256::from(amount),
            nonce: U256::from(nonce),
            bytes_delivered: U256::from(bytes),
        }
    }

    #[test]
    fn accepts_waiver_at_authorized_watermark() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0x11);
        let token = Address::repeat_byte(0x22);
        let auth = provider_auth(
            &provider,
            channel_id,
            token,
            U256::from(500u64),
            U256::from(3u64),
            U256::from(9000u64),
        );
        let prepared = prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect("prepare at exact watermark");
        assert_eq!(prepared.amount, U256::from(500u64));
        // The client sig must recover to the client key over the same tuple.
        let recovered = SignedVoucherCheck::recover(
            &prepared.client_sig,
            channel_id,
            U256::from(500u64),
            U256::from(3u64),
            U256::from(9000u64),
            token,
        );
        assert_eq!(recovered, client.address());
    }

    #[test]
    fn accepts_provider_watermark_below_authorized() {
        // Provider acked fewer bytes than the client issued — settling at the
        // provider's lower tuple is fine (client pays less).
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0x33);
        let token = Address::repeat_byte(0x44);
        let auth = provider_auth(
            &provider,
            channel_id,
            token,
            U256::from(400u64),
            U256::from(2u64),
            U256::from(8000u64),
        );
        prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect("provider tuple below authorized is accepted");
    }

    #[test]
    fn refuses_provider_over_claim() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0x55);
        let token = Address::repeat_byte(0x66);
        // Provider asks for MORE than the client authorized.
        let auth = provider_auth(
            &provider,
            channel_id,
            token,
            U256::from(600u64),
            U256::from(3u64),
            U256::from(9000u64),
        );
        let err = prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect_err("over-claim must be refused");
        assert!(err.to_string().contains("over-claimed"), "{err}");
    }

    #[test]
    fn refuses_waiver_from_wrong_signer() {
        let provider = PrivateKeySigner::random();
        let impostor = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0x77);
        let token = Address::repeat_byte(0x88);
        // Waiver signed by the impostor, but we expect the real provider.
        let auth = provider_auth(
            &impostor,
            channel_id,
            token,
            U256::from(500u64),
            U256::from(3u64),
            U256::from(9000u64),
        );
        let err = prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect_err("wrong-signer waiver must be refused");
        assert!(err.to_string().contains("verification failed"), "{err}");
    }

    /// Recover the signer of a wire voucher signature over a tuple — test helper
    /// mirroring the on-chain client-sig check.
    struct SignedVoucherCheck;
    impl SignedVoucherCheck {
        fn recover(
            sig: &Bytes,
            channel_id: B256,
            amount: U256,
            nonce: U256,
            bytes_delivered: U256,
            token: Address,
        ) -> Address {
            let signature = Signature::try_from(sig.as_ref()).expect("parse client sig");
            decdn_incentive::SignedVoucher {
                voucher: Voucher {
                    channel_id,
                    amount,
                    nonce,
                    bytes_delivered,
                    token,
                },
                signature,
            }
            .recover_signer(&domain())
            .expect("recover client sig")
        }
    }
}
