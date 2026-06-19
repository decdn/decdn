//! EIP-712 cooperative-close waivers for the `PaymentChannel` contract.
//!
//! A cooperative close lets a channel settle in one transaction with no dispute
//! window when both parties agree on the final state (ADR 003 §Cooperative
//! close). The client's ordinary [`crate::voucher::Voucher`] caps the amount;
//! the **provider** signs a `CooperativeClose` waiver over the same final
//! `(channelId, amount, nonce, bytesDelivered, token)` tuple, attesting it holds
//! no higher voucher and waiving the window.
//!
//! The waiver shares the voucher's EIP-712 domain (see
//! [`crate::voucher::voucher_domain`]) — `PaymentChannel` recovers both against
//! the same `_hashTypedDataV4` domain separator. Only the type-string differs:
//!
//! ```text
//! CooperativeClose(bytes32 channelId,uint256 amount,uint256 nonce,
//!                  uint256 bytesDelivered,address token)
//! ```

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::SolStruct;

pub use crate::voucher::{VoucherError, voucher_domain};

// Solidity struct mirroring `PaymentChannel.COOPERATIVE_CLOSE_TYPEHASH`. The
// `sol!` macro uses the Rust struct name as the on-chain type name in the
// EIP-712 type-string, so it must be `CooperativeClose` verbatim; the wrapping
// module avoids clashing with the public Rust [`CooperativeClose`] below.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct CooperativeClose {
            bytes32 channelId;
            uint256 amount;
            uint256 nonce;
            uint256 bytesDelivered;
            address token;
        }
    }
}

use sol_types::CooperativeClose as CooperativeCloseSol;

/// A provider's cooperative-close waiver in its unsigned form. Field semantics
/// match [`crate::voucher::Voucher`]: all amounts are cumulative over the
/// channel's lifetime and describe the agreed final state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooperativeClose {
    /// `channelId = keccak256(client, provider, channelNonce)` per ADR 003.
    pub channel_id: B256,
    /// Cumulative final payment in token base units (`µUSDC` for `USDC`).
    pub amount: U256,
    /// Final voucher sequence number within the channel.
    pub nonce: U256,
    /// Cumulative final bytes delivered against this channel.
    pub bytes_delivered: U256,
    /// `ERC-20` token address (`USDC` — the only token bound by `PaymentChannel`).
    pub token: Address,
}

impl CooperativeClose {
    const fn to_sol(&self) -> CooperativeCloseSol {
        CooperativeCloseSol {
            channelId: self.channel_id,
            amount: self.amount,
            nonce: self.nonce,
            bytesDelivered: self.bytes_delivered,
            token: self.token,
        }
    }

    /// EIP-712 signing hash bound to `domain` — the 32-byte digest the contract
    /// recovers the provider waiver against.
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        self.to_sol().eip712_signing_hash(domain)
    }

    /// Sign the waiver with the provider's `signer` for the given EIP-712
    /// `domain`.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying signer (key locked,
    /// remote signer offline, etc.).
    pub fn sign<S: SignerSync>(
        self,
        signer: &S,
        domain: &Eip712Domain,
    ) -> Result<SignedCooperativeClose, alloy::signers::Error> {
        let hash = self.signing_hash(domain);
        let signature = signer.sign_hash_sync(&hash)?;
        Ok(SignedCooperativeClose {
            close: self,
            signature,
        })
    }
}

/// A cooperative-close waiver together with its EIP-712 signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCooperativeClose {
    pub close: CooperativeClose,
    pub signature: Signature,
}

impl SignedCooperativeClose {
    /// Recover the address that signed this waiver under `domain`.
    ///
    /// # Errors
    ///
    /// Returns [`VoucherError::InvalidSignature`] if the signature is malformed
    /// (non-canonical high-`s`, invalid recovery id, etc.) — rejecting high-`s`
    /// up front so the off-chain accept-set matches the on-chain verifiable-set
    /// (#836), exactly as voucher recovery does.
    pub fn recover_signer(&self, domain: &Eip712Domain) -> Result<Address, VoucherError> {
        if crate::sig_canon::is_high_s(&self.signature) {
            return Err(VoucherError::InvalidSignature);
        }
        let hash = self.close.signing_hash(domain);
        self.signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| VoucherError::InvalidSignature)
    }

    /// Verify the waiver was produced by `expected` (the channel's provider) for
    /// `domain`.
    ///
    /// # Errors
    ///
    /// - [`VoucherError::InvalidSignature`] — signature is malformed.
    /// - [`VoucherError::WrongSigner`] — recovered address differs from
    ///   `expected`.
    pub fn verify_signer(
        &self,
        expected: Address,
        domain: &Eip712Domain,
    ) -> Result<(), VoucherError> {
        let recovered = self.recover_signer(domain)?;
        if recovered == expected {
            Ok(())
        } else {
            Err(VoucherError::WrongSigner {
                expected,
                recovered,
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256, keccak256};
    use alloy::signers::local::PrivateKeySigner;

    fn sample_close() -> CooperativeClose {
        CooperativeClose {
            channel_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            amount: U256::from(10_000_000u64),
            nonce: U256::from(3u64),
            bytes_delivered: U256::from(1_048_576u64),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
        }
    }

    fn sample_domain() -> Eip712Domain {
        voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    #[test]
    fn round_trip_sign_verify() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let signed = sample_close().sign(&signer, &domain)?;
        signed.verify_signer(signer.address(), &domain)?;
        Ok(())
    }

    #[test]
    fn wrong_signer_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let other = PrivateKeySigner::random().address();
        let domain = sample_domain();
        let signed = sample_close().sign(&signer, &domain)?;
        anyhow::ensure!(
            matches!(
                signed.verify_signer(other, &domain),
                Err(VoucherError::WrongSigner { .. })
            ),
            "expected WrongSigner"
        );
        Ok(())
    }

    #[test]
    fn high_s_signature_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let signed = sample_close().sign(&signer, &domain)?;
        let twin = SignedCooperativeClose {
            signature: crate::sig_canon::high_s_twin(&signed.signature),
            ..signed
        };
        anyhow::ensure!(
            twin.recover_signer(&domain) == Err(VoucherError::InvalidSignature),
            "high-s waiver twin must be rejected"
        );
        Ok(())
    }

    /// Lock the EIP-712 type hash to the exact `PaymentChannel.COOPERATIVE_CLOSE_TYPEHASH`
    /// wording. If this breaks, the contract typehash or the `sol!` canonical
    /// encoding drifted — a coordinated update with the Solidity contract.
    #[test]
    fn cooperative_close_type_hash_matches_contract() -> anyhow::Result<()> {
        let canonical: &[u8] = b"CooperativeClose(bytes32 channelId,uint256 amount,uint256 nonce,uint256 bytesDelivered,address token)";
        let expected = keccak256(canonical);
        let actual = CooperativeCloseSol::eip712_type_hash(&sample_close().to_sol());
        anyhow::ensure!(
            actual == expected,
            "cooperative-close type hash drifted: actual={actual} expected={expected}"
        );
        Ok(())
    }

    /// A client voucher and a provider waiver over the *same* tuple must produce
    /// *different* digests — the distinct type-string is what stops one standing
    /// in for the other (mirrors the on-chain `_verifyCooperativeClose` vs
    /// `_verifyVoucher` split).
    #[test]
    fn waiver_digest_differs_from_voucher_digest() -> anyhow::Result<()> {
        use crate::voucher::Voucher;
        let domain = sample_domain();
        let c = sample_close();
        let voucher = Voucher {
            channel_id: c.channel_id,
            amount: c.amount,
            nonce: c.nonce,
            bytes_delivered: c.bytes_delivered,
            token: c.token,
        };
        anyhow::ensure!(
            c.signing_hash(&domain) != voucher.signing_hash(&domain),
            "waiver and voucher digests must differ (distinct typehash)"
        );
        Ok(())
    }
}
