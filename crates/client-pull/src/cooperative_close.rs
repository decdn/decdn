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
//! 2. Bound the provider's returned tuple by what the client authorized — or,
//!    when it exceeds that, by the client's own signature over exactly that
//!    tuple, echoed back in `CooperativeCloseAuth::last_signature` (#1495). Then
//!    verify the waiver signature really is the provider's.
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
use decdn_incentive::{CooperativeClose, SignedCooperativeClose, Voucher, sign_coop_close_request};
use decdn_protocol::client::{ClientMessage, CooperativeCloseAuth, CooperativeCloseRequest};
use decdn_protocol::{
    ALPN_CLIENT, FrameError, decode_message, encode_message, read_frame, write_frame,
};
use iroh::{Endpoint, EndpointAddr};

use crate::voucher_signed_by;

/// What the client authorized on the channel — the cap the provider's returned
/// settlement tuple may not exceed.
///
/// The on-chain `cooperativeClose` already bounds `amount` by the deposit and by
/// the monotonic on-chain `claimed*` watermark, but neither stops a provider
/// from asking the client to *sign away* more than it actually owes (up to the
/// full deposit). This cap is the off-chain guard: the client refuses to sign a
/// voucher above what it issued — **unless** the provider proves, with the
/// client's own signature over exactly the declared tuple, that the client
/// already issued the higher one (#1495; see `prepare_close`). For a
/// buyer-held channel these come straight from the persisted
/// `BuyerChannelState` (`last_amount`/`last_nonce`/`last_bytes_delivered`),
/// which is exactly the record that can lag what was actually signed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// The request is authenticated: `client_signer` (the channel's `voucherSigner`
/// key — the same key that signs the settlement voucher below) signs an EIP-712
/// `CooperativeCloseRequest(channelId)` under `domain` (the `PaymentChannel`
/// voucher domain), which the provider recovers and checks against the channel's
/// pinned `voucherSigner` before signing any waiver. Without it the provider
/// declines — the same `Ok(None)` path as an unknown channel.
///
/// Returns `Ok(Some(auth))` with the provider's waiver (whose `last_signature` is
/// empty when the node supplied no echo — see [`CooperativeCloseAuth`]), or
/// `Ok(None)` when the provider finishes the stream without a waiver — its
/// decline signal for an unknown channel, an unauthenticated request, or a
/// channel with no accepted voucher yet (handlers/client.rs). A network/protocol
/// failure is `Err`.
///
/// # Errors
///
/// Dial/stream failures, request signing, a non-auth reply, a malformed auth
/// frame, or timeout.
pub async fn request_cooperative_close_auth(
    endpoint: &Endpoint,
    target: EndpointAddr,
    channel_id: B256,
    client_signer: &PrivateKeySigner,
    domain: &Eip712Domain,
    timeout: Duration,
) -> anyhow::Result<Option<CooperativeCloseAuth>> {
    let client_signature = sign_coop_close_request(client_signer, channel_id, domain)
        .map_err(|e| anyhow::anyhow!("sign cooperative-close request: {e}"))?;
    tokio::time::timeout(
        timeout,
        request_inner(endpoint, target, channel_id, client_signature),
    )
    .await
    .map_err(|_| anyhow::anyhow!("cooperative-close request timed out after {timeout:?}"))?
}

async fn request_inner(
    endpoint: &Endpoint,
    target: EndpointAddr,
    channel_id: B256,
    client_signature: Vec<u8>,
) -> anyhow::Result<Option<CooperativeCloseAuth>> {
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
        client_signature,
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
    let (msg, _remainder) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode auth: {e}"))?;
    match msg {
        ClientMessage::CooperativeCloseAuth(auth) => {
            // `validate` checks both signature lengths, including the #1495
            // watermark echo now carried inside the message.
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
    /// Provider `CooperativeClose` waiver signature (`r‖s‖v`, `v` in 27/28),
    /// re-serialized from the parsed auth signature.
    provider_sig: Bytes,
}

/// Validate the provider's waiver against what the client authorized, then sign
/// the matching client voucher — the security-critical, network-free core.
///
/// The provider's tuple is accepted when it is **within** `authorized`, or — when
/// it exceeds it — when `auth.last_signature` proves the client itself already
/// signed **that exact tuple** (#1495). Everything else is refused: a provider
/// trying to inflate the payout toward the full deposit, or a waiver that does
/// not recover to `provider_eth`.
///
/// The second branch is what lets a client whose persisted watermark legitimately
/// lags — it signed vouchers it did not durably persist before an unclean exit —
/// cooperatively close at all. Without it the guard is correct but terminal: the
/// caller keeps the `closeChannel` → dispute window → `settleChannel` fallback,
/// so nothing is lost, but it forfeits the one-transaction settle and submits a
/// voucher *below* what it actually signed, underpaying the provider unless the
/// provider watches the window and disputes. The branch is not a relaxation:
/// proving the client signed the tuple is strictly stronger evidence than the
/// local record it replaces, and it is checked over the same tuple this function
/// goes on to sign, so there is no window between what was proved and what is
/// committed.
///
/// Returns the prepared close and, when the second branch was taken, the healed
/// watermark for the caller to persist.
///
/// # Errors
///
/// - the provider echoed a waiver for a different `channel_id`,
/// - the provider over-claimed beyond `authorized` and supplied no signature of
///   ours over the declared tuple,
/// - the waiver signature is malformed or signed by the wrong key,
/// - client voucher signing fails.
#[allow(clippy::too_many_arguments)]
fn prepare_close(
    auth: &CooperativeCloseAuth,
    channel_id: B256,
    provider_eth: Address,
    token: Address,
    authorized: AuthorizedWatermark,
    client_signer: &PrivateKeySigner,
    domain: &Eip712Domain,
) -> anyhow::Result<(PreparedClose, Option<AuthorizedWatermark>)> {
    // The provider echoes the requested `channel_id`; a mismatch would already
    // fail signature verification (the digest is rebuilt from the local
    // `channel_id`), but checking the echo up front gives an actionable error
    // instead of an opaque "verification failed".
    if B256::from(auth.channel_id) != channel_id {
        anyhow::bail!(
            "provider returned a waiver for the wrong channel: expected {channel_id}, got {}",
            B256::from(auth.channel_id)
        );
    }

    let amount = U256::from_be_bytes(auth.amount);
    let nonce = U256::from_be_bytes(auth.nonce);
    let bytes_delivered = U256::from_be_bytes(auth.bytes_delivered);

    // The one statement of the tuple: proved below, then signed below that, so
    // "what was verified" and "what is committed" are the same VALUE rather than
    // two locals that happen to agree.
    let settlement = Voucher {
        channel_id,
        amount,
        nonce,
        bytes_delivered,
        token,
    };

    // Never sign away more than we can account for. The provider returns the
    // highest client voucher it holds, which can only be <= what we issued — so a
    // larger tuple is either a misbehaving provider inflating the payout, or our
    // own record lagging what we actually signed.
    //
    // Tell those apart by asking the provider to prove it: `last_signature` must
    // recover to OUR voucher-signing key over exactly `settlement`. Only we could
    // have produced it, so a tuple that verifies is one we already committed to
    // and the record — not the waiver — is what was wrong. Anything that does not
    // verify is refused as before; the channel can still be closed the slow way.
    let over_claimed = amount > authorized.amount
        || nonce > authorized.nonce
        || bytes_delivered > authorized.bytes_delivered;
    let mut reconciled = None;
    if over_claimed {
        if !voucher_signed_by(
            &settlement,
            &auth.last_signature,
            client_signer.address(),
            domain,
        ) {
            // Distinguish the two causes: an operator's next action differs.
            let detail = if auth.last_signature.is_empty() {
                "and the provider echoed no signature of ours (it holds no stored signature \
                 for this channel) — close it the ordinary way"
            } else {
                "and the signature it echoed is not one of ours over that tuple — this provider \
                 is claiming payment we never authorized"
            };
            anyhow::bail!(
                "provider over-claimed: waiver ({amount}, {nonce}, {bytes_delivered}) exceeds \
                 authorized ({}, {}, {}) {detail}",
                authorized.amount,
                authorized.nonce,
                authorized.bytes_delivered
            );
        }
        reconciled = Some(AuthorizedWatermark {
            amount,
            nonce,
            bytes_delivered,
        });
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

    // Sign the agreed tuple — the same `settlement` value proved above, and the
    // client sig the contract recovers against the channel's pinned
    // `voucherSigner`.
    let client_sig = settlement
        .sign(client_signer, domain)
        .map_err(|e| anyhow::anyhow!("client voucher signing failed: {e}"))?
        .signature;

    Ok((
        PreparedClose {
            amount,
            nonce,
            bytes_delivered,
            // `Signature::as_bytes` emits `v` in the 27/28 convention the contract's
            // `ECDSA.recover` expects (see node `normalize_voucher_signature`). Use the
            // re-serialized parsed `provider_sig` rather than the raw wire bytes so a
            // non-canonical `v` (0/1) that parses and verifies still lands on-chain in
            // the form the contract requires — both sigs go through the same normalizer.
            client_sig: Bytes::from(client_sig.as_bytes().to_vec()),
            provider_sig: Bytes::from(provider_sig.as_bytes().to_vec()),
        },
        reconciled,
    ))
}

/// Run a full client-initiated cooperative close against `target` and submit it
/// on-chain through `contract`.
///
/// Requests the waiver, validates + signs it against `authorized`, and submits
/// `cooperativeClose`. The contract enforces the deposit ceiling and monotonic
/// `claimed*` floor, so a revert is non-fatal ([`CooperativeCloseOutcome`]) — the
/// caller falls back to `closeChannel`.
///
/// `reconciled` is an out-param, set to `Some` as soon as the provider proves —
/// with the client's own signature over exactly the declared tuple — that the
/// client signed a HIGHER watermark than `authorized` (#1495). It is written
/// **before** the transaction is sent, so it is reported on every return path
/// including the error ones, mirroring `stream_fetch_tracked`'s `progress`
/// out-param. That matters: a `get_receipt` failure does not mean the settle
/// failed, so a caller that only learned the healed watermark from an `Ok` could
/// keep a stale record for a chain that already settled at the higher tuple.
/// Persisting it is safe regardless of how the transaction resolves — it is a
/// tuple this client provably signed, and `advance_progress` is monotonic.
///
/// The caller MUST persist it: on a settle so a failed channel-forget cannot
/// leave the store claiming less than was settled, and on a revert or error so
/// the `closeChannel` fallback submits the true voucher rather than the stale one.
///
/// # Errors
///
/// Network/protocol failure, an unproved provider over-claim, waiver
/// verification failure, signing failure, or an on-chain send/receipt error.
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
    reconciled: &mut Option<AuthorizedWatermark>,
) -> anyhow::Result<CooperativeCloseOutcome> {
    let Some(auth) = request_cooperative_close_auth(
        endpoint,
        target,
        channel_id,
        client_signer,
        domain,
        timeout,
    )
    .await?
    else {
        return Ok(CooperativeCloseOutcome::Declined);
    };

    let (prepared, healed) = prepare_close(
        &auth,
        channel_id,
        provider_eth,
        token,
        authorized,
        client_signer,
        domain,
    )?;
    // Report before sending, so every path below — including the two error
    // returns — hands the caller a watermark it has already proved it signed.
    *reconciled = healed;

    let pending = match contract
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
    {
        Ok(pending) => pending,
        // A deterministic revert (raced `withdraw`/`closeChannel`, or a watermark
        // regressed against the on-chain `claimed*`) is caught at gas estimation,
        // so it never reaches a mined receipt — it surfaces here with revert data
        // attached. That's the non-fatal `Reverted` outcome, so the caller can
        // fall back to `closeChannel`. A send error *without* revert data is a
        // genuine transport/RPC failure (connectivity, nonce) and stays an error
        // the caller should retry, not a settlement signal.
        Err(e) if e.as_revert_data().is_some() => {
            return Ok(CooperativeCloseOutcome::Reverted);
        }
        Err(e) => return Err(anyhow::anyhow!("cooperativeClose send failed: {e}")),
    };
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
            last_signature: Vec::new(),
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
        let (prepared, reconciled) = prepare_close(
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
        assert_eq!(
            reconciled, None,
            "nothing to reconcile at the exact watermark"
        );
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
        let (_, reconciled) = prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect("provider tuple below authorized is accepted");
        assert_eq!(reconciled, None, "a lower tuple is not a reconciliation");
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

    #[test]
    fn refuses_waiver_for_wrong_channel() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let token = Address::repeat_byte(0x99);
        // Provider returns a (well-signed) waiver for a different channel than the
        // one we asked to close.
        let auth = provider_auth(
            &provider,
            B256::repeat_byte(0xAA),
            token,
            U256::from(500u64),
            U256::from(3u64),
            U256::from(9000u64),
        );
        let err = prepare_close(
            &auth,
            B256::repeat_byte(0xBB),
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect_err("wrong-channel waiver must be refused");
        assert!(err.to_string().contains("wrong channel"), "{err}");
    }

    /// Build the echo a node attaches (`CooperativeCloseAuth::last_signature`):
    /// `signer`'s voucher signature over `(channel_id, amount, nonce, bytes,
    /// token)`. With `signer == the client` and the tuple matching the auth's,
    /// this is the genuine #1495 echo.
    fn echo(
        signer: &PrivateKeySigner,
        channel_id: B256,
        token: Address,
        amount: U256,
        nonce: U256,
        bytes_delivered: U256,
    ) -> Vec<u8> {
        let voucher = Voucher {
            channel_id,
            amount,
            nonce,
            bytes_delivered,
            token,
        }
        .sign(signer, &domain())
        .expect("voucher signs");
        voucher.signature.as_bytes().to_vec()
    }

    /// The #1495 reconcile: our persisted record lags what we actually signed, and
    /// the provider proves the higher tuple with our own signature over it. We
    /// settle at the provider's state instead of dead-ending with the deposit
    /// stranded.
    #[test]
    fn reconciles_over_claim_proved_by_our_own_signature() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0xC1);
        let token = Address::repeat_byte(0xC2);
        let (amount, nonce, bytes) = (U256::from(600u64), U256::from(4u64), U256::from(9500u64));
        let mut auth = provider_auth(&provider, channel_id, token, amount, nonce, bytes);
        auth.last_signature = echo(&client, channel_id, token, amount, nonce, bytes);

        let (prepared, reconciled) = prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            // Our record lags: we persisted only up to (500, 3, 9000).
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect("a self-signed over-claim reconciles");

        assert_eq!(prepared.amount, amount, "settles at the provider's state");
        assert_eq!(
            reconciled,
            Some(AuthorizedWatermark {
                amount,
                nonce,
                bytes_delivered: bytes,
            }),
            "the healed watermark is surfaced for the caller to persist"
        );
    }

    /// An over-claim with a signature from someone else's key is exactly the
    /// deposit-draining attack the guard exists to stop. Still refused.
    #[test]
    fn refuses_over_claim_signed_by_a_foreign_key() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let impostor = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0xD1);
        let token = Address::repeat_byte(0xD2);
        let (amount, nonce, bytes) = (U256::from(600u64), U256::from(4u64), U256::from(9500u64));
        let mut auth = provider_auth(&provider, channel_id, token, amount, nonce, bytes);
        // Signed by someone who is not us — proves nothing about what we owe.
        auth.last_signature = echo(&impostor, channel_id, token, amount, nonce, bytes);

        let err = prepare_close(
            &auth,
            channel_id,
            provider.address(),
            token,
            watermark(500, 3, 9000),
            &client,
            &domain(),
        )
        .expect_err("a foreign signature must not unlock the over-claim");
        assert!(err.to_string().contains("over-claimed"), "{err}");
        // The two refusal causes must read differently: a hostile echo tells the
        // operator to stop using this provider, an absent one only to close the
        // slow way. Same string for both would bury the security event.
        assert!(
            err.to_string().contains("not one of ours over that tuple"),
            "a present-but-foreign echo must be named as such: {err}"
        );
    }

    /// The echo must cover the tuple being settled, not merely be *some* voucher
    /// we once signed — otherwise a provider could pair a low real signature with
    /// an inflated declared tuple.
    #[test]
    fn refuses_over_claim_whose_echo_covers_a_different_tuple() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0xE1);
        let token = Address::repeat_byte(0xE2);
        // Declared: 600. Echoed signature: a genuine one of ours, but over 500.
        let mut auth = provider_auth(
            &provider,
            channel_id,
            token,
            U256::from(600u64),
            U256::from(4u64),
            U256::from(9500u64),
        );
        auth.last_signature = echo(
            &client,
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
        .expect_err("an echo over a different tuple must not unlock the over-claim");
        assert!(err.to_string().contains("over-claimed"), "{err}");
    }

    /// A node holding no stored signature for the channel sends an empty echo.
    /// The client cannot verify, so it keeps the pre-#1495 refusal — absence is
    /// never weaker.
    #[test]
    fn refuses_over_claim_when_the_node_sends_no_echo() {
        let provider = PrivateKeySigner::random();
        let client = PrivateKeySigner::random();
        let channel_id = B256::repeat_byte(0xF1);
        let token = Address::repeat_byte(0xF2);
        let auth = provider_auth(
            &provider,
            channel_id,
            token,
            U256::from(600u64),
            U256::from(4u64),
            U256::from(9500u64),
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
        .expect_err("no echo ⇒ the pre-#1495 refusal stands");
        assert!(err.to_string().contains("over-claimed"), "{err}");
        assert!(
            err.to_string().contains("echoed no signature of ours"),
            "an absent echo must be distinguishable from a hostile one: {err}"
        );
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
