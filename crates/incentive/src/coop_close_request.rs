//! EIP-712 authenticator for a cooperative-close **request** (ADR 003
//! §Cooperative close).
//!
//! A [`crate::cooperative_close::CooperativeClose`] waiver commits the provider
//! to settling at a channel's watermark and to serving no further bytes on it.
//! Because that commitment is durable and one-way, the node MUST NOT sign it for
//! anyone but the channel's pinned `voucherSigner` — otherwise any peer that can
//! name a channel id (which is chain-derivable) could freeze the channel. The
//! request therefore carries a signature the requester produces with the
//! channel's `voucherSigner` key; the node recovers it and refuses the waiver
//! unless it recovers to `channel.voucherSigner` (the SIGNER identity, matching
//! the paid path and on-chain `cooperativeClose`, never the funder `client`).
//!
//! This signature is **off-chain only** — it is never submitted to any contract,
//! so there is no on-chain typehash to match (unlike the voucher and the waiver).
//! It shares the voucher's EIP-712 domain (see [`crate::voucher::voucher_domain`])
//! so it is bound to the same chain + `PaymentChannel` deployment, and its
//! distinct type-string keeps it from standing in for a voucher or a waiver:
//!
//! ```text
//! CooperativeCloseRequest(bytes32 channelId)
//! ```
//!
//! Only EOA (`ecrecover`) verification is implemented, matching the EOA-only
//! off-chain signing stance of [`crate::bind_sig`], [`crate::voucher`], and
//! [`crate::cooperative_close`]. Non-canonical high-`s` signatures are rejected
//! so the off-chain accept-set matches the on-chain verifiable-set (#836).

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::SignerSync;
use alloy::sol_types::SolStruct;

pub use crate::voucher::voucher_domain;

/// Exact byte length of a cooperative-close request signature (`r‖s‖v`,
/// 32+32+1) — the EOA off-chain signing form shared with the voucher, waiver,
/// and binding signatures.
pub const COOP_CLOSE_REQUEST_SIG_LEN: usize = 65;

// Solidity-shaped struct so the EIP-712 type-string is fixed and greppable. It
// mirrors nothing on-chain (the request never reaches a contract); the `sol!`
// macro is used only to derive the canonical typehash + struct encoding. Wrapped
// in a private module so the name does not clash with the wire
// `decdn_protocol::client::CooperativeCloseRequest`.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct CooperativeCloseRequest {
            bytes32 channelId;
        }
    }
}

use sol_types::CooperativeCloseRequest as CooperativeCloseRequestSol;

/// EIP-712 signing hash for a `CooperativeCloseRequest(channelId)` under
/// `domain` — the digest the node recovers the requester's address against.
#[must_use]
pub fn coop_close_request_signing_hash(channel_id: B256, domain: &Eip712Domain) -> B256 {
    CooperativeCloseRequestSol {
        channelId: channel_id,
    }
    .eip712_signing_hash(domain)
}

/// Sign a cooperative-close request for `channel_id` with the requester's
/// `signer` under `domain`, returning the 65-byte (`r‖s‖v`) wire signature.
///
/// # Errors
///
/// Propagates any error from the underlying signer (key locked, etc.).
pub fn sign_coop_close_request<S: SignerSync>(
    signer: &S,
    channel_id: B256,
    domain: &Eip712Domain,
) -> Result<Vec<u8>, alloy::signers::Error> {
    let hash = coop_close_request_signing_hash(channel_id, domain);
    Ok(signer.sign_hash_sync(&hash)?.as_bytes().to_vec())
}

/// Recover the Ethereum address that signed a cooperative-close request for
/// `channel_id` under `domain` (EOA `ecrecover`). The caller MUST check the
/// recovered address equals the channel's pinned `voucherSigner` before honoring
/// the request.
///
/// # Errors
///
/// - [`CoopCloseRequestError::BadLength`] — `signature` is not 65 bytes.
/// - [`CoopCloseRequestError::Malformed`] — well-formed length but non-canonical
///   `s` / invalid recovery id, so no address can be recovered.
pub fn recover_coop_close_request(
    channel_id: B256,
    signature: &[u8],
    domain: &Eip712Domain,
) -> Result<Address, CoopCloseRequestError> {
    if signature.len() != COOP_CLOSE_REQUEST_SIG_LEN {
        return Err(CoopCloseRequestError::BadLength {
            len: signature.len(),
        });
    }
    let sig = Signature::from_raw(signature).map_err(|_| CoopCloseRequestError::Malformed)?;
    // Reject non-canonical high-`s` so the off-chain accept-set matches the
    // on-chain verifiable-set (#836) — the same stance as `verify_binding`.
    if crate::sig_canon::is_high_s(&sig) {
        return Err(CoopCloseRequestError::Malformed);
    }
    let hash = coop_close_request_signing_hash(channel_id, domain);
    sig.recover_address_from_prehash(&hash)
        .map_err(|_| CoopCloseRequestError::Malformed)
}

/// Failure modes for [`recover_coop_close_request`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoopCloseRequestError {
    /// Signature is not the 65-byte EOA form.
    #[error("cooperative-close request signature has invalid length {len} (EOA form is 65 bytes)")]
    BadLength { len: usize },
    /// Signature is 65 bytes but malformed — non-canonical `s` or invalid
    /// recovery id; no address could be recovered.
    #[error("cooperative-close request signature is malformed")]
    Malformed,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256, keccak256};
    use alloy::signers::local::PrivateKeySigner;

    fn sample_channel_id() -> B256 {
        b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff")
    }

    fn sample_domain() -> Eip712Domain {
        voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    #[test]
    fn round_trip_sign_recover() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let sig = sign_coop_close_request(&signer, sample_channel_id(), &domain)?;
        let recovered = recover_coop_close_request(sample_channel_id(), &sig, &domain)?;
        anyhow::ensure!(recovered == signer.address(), "must recover the signer");
        Ok(())
    }

    #[test]
    fn wrong_channel_recovers_a_different_address() -> anyhow::Result<()> {
        // A signature over channel A cannot authenticate a request for channel B:
        // the digest differs, so recovery yields some *other* address, never the
        // signer's — which is exactly what the caller's `== channel.client` check
        // then rejects.
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let sig = sign_coop_close_request(&signer, sample_channel_id(), &domain)?;
        let other_channel =
            b256!("aaaa000000000000000000000000000000000000000000000000000000000000");
        let recovered = recover_coop_close_request(other_channel, &sig, &domain)?;
        anyhow::ensure!(
            recovered != signer.address(),
            "a request signature for a different channel must not recover the signer"
        );
        Ok(())
    }

    #[test]
    fn bad_length_rejected() {
        let domain = sample_domain();
        let err = recover_coop_close_request(sample_channel_id(), &[0u8; 64], &domain)
            .expect_err("64-byte signature must be rejected");
        assert!(matches!(err, CoopCloseRequestError::BadLength { len: 64 }));
    }

    #[test]
    fn empty_signature_rejected() {
        let domain = sample_domain();
        // The wire "absent signature" (empty vec) is a length error, so the node
        // declines it rather than treating it as a valid request.
        let err = recover_coop_close_request(sample_channel_id(), &[], &domain)
            .expect_err("empty signature must be rejected");
        assert!(matches!(err, CoopCloseRequestError::BadLength { len: 0 }));
    }

    #[test]
    fn high_s_signature_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let sig = sign_coop_close_request(&signer, sample_channel_id(), &domain)?;
        let parsed = Signature::from_raw(&sig)?;
        let twin = crate::sig_canon::high_s_twin(&parsed).as_bytes().to_vec();
        anyhow::ensure!(
            recover_coop_close_request(sample_channel_id(), &twin, &domain)
                == Err(CoopCloseRequestError::Malformed),
            "high-s twin must be rejected"
        );
        Ok(())
    }

    /// Lock the EIP-712 type-string. This authenticator is off-chain only, so
    /// there is no contract counterpart — but pinning the typehash keeps the
    /// signed payload from silently drifting (which would invalidate every
    /// in-flight request signature) and keeps it distinct from the voucher /
    /// waiver type-strings so one can never stand in for another.
    #[test]
    fn type_hash_is_stable_and_distinct() -> anyhow::Result<()> {
        let canonical: &[u8] = b"CooperativeCloseRequest(bytes32 channelId)";
        let expected = keccak256(canonical);
        let actual = CooperativeCloseRequestSol::eip712_type_hash(&CooperativeCloseRequestSol {
            channelId: sample_channel_id(),
        });
        anyhow::ensure!(actual == expected, "request type hash drifted");
        // Distinct from the waiver digest over the same domain.
        let waiver = crate::cooperative_close::CooperativeClose {
            channel_id: sample_channel_id(),
            amount: alloy::primitives::U256::from(1u64),
            nonce: alloy::primitives::U256::from(1u64),
            bytes_delivered: alloy::primitives::U256::from(1u64),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        };
        anyhow::ensure!(
            coop_close_request_signing_hash(sample_channel_id(), &sample_domain())
                != waiver.signing_hash(&sample_domain()),
            "request digest must differ from the waiver digest"
        );
        Ok(())
    }
}
