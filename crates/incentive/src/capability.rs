//! EIP-712 spending capabilities for the `PaymentPool` contract.
//!
//! A capability is an owner-signed grant that delegates spend on a pool to a
//! `signer` key up to a `spending_cap`, until `expiry`. The pool owner (the
//! funder of the on-chain deposit) signs it; the node registers it and then
//! accepts [`crate::voucher::Voucher`]s from that `signer` up to the cap
//! (ADR 003 §Capability delegation).
//!
//! The capability shares the voucher's EIP-712 domain (see
//! [`crate::voucher::voucher_domain`]) — `PaymentPool` recovers both against
//! the same `_hashTypedDataV4` domain separator. Only the type-string differs:
//!
//! ```text
//! Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)
//! ```

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::SolStruct;

pub use crate::voucher::voucher_domain;

// Solidity struct mirroring `PaymentPool.CAPABILITY_TYPEHASH`. The `sol!`
// macro uses the Rust struct name as the on-chain type name in the EIP-712
// type-string, so it must be `Capability` verbatim; the wrapping module avoids
// clashing with the public Rust [`Capability`] below.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct Capability {
            address signer;
            uint256 spendingCap;
            bytes32 poolId;
            uint64 expiry;
        }
    }
}

use sol_types::Capability as CapabilitySol;

/// A pool owner's spending grant in its unsigned form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    /// The address the owner delegates spend to — the voucher-signing key.
    pub signer: Address,
    /// Maximum cumulative amount (token base units) the `signer` may spend
    /// against `pool_id` under this grant.
    pub spending_cap: U256,
    /// The pool this capability draws from — the on-chain `PaymentPool`
    /// deposit id.
    pub pool_id: B256,
    /// Unix-seconds expiry. After it, vouchers under this capability are no
    /// longer redeemable and the node stops accepting them.
    pub expiry: u64,
}

impl Capability {
    const fn to_sol(&self) -> CapabilitySol {
        CapabilitySol {
            signer: self.signer,
            spendingCap: self.spending_cap,
            poolId: self.pool_id,
            expiry: self.expiry,
        }
    }

    /// EIP-712 signing hash bound to `domain` — the 32-byte digest the contract
    /// recovers the owner signature against.
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        self.to_sol().eip712_signing_hash(domain)
    }

    /// Sign the capability with the pool owner's `signer` for the given EIP-712
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
    ) -> Result<SignedCapability, alloy::signers::Error> {
        let hash = self.signing_hash(domain);
        let signature = signer.sign_hash_sync(&hash)?;
        Ok(SignedCapability {
            capability: self,
            signature,
        })
    }
}

/// A capability together with its EIP-712 owner signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCapability {
    pub capability: Capability,
    pub signature: Signature,
}

impl SignedCapability {
    /// Recover the owner address that signed this capability under `domain`.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityError::InvalidSignature`] if the signature is
    /// malformed (non-canonical high-`s`, invalid recovery id, etc.) —
    /// rejecting high-`s` up front so the off-chain accept-set matches the
    /// on-chain verifiable-set (#836), exactly as voucher recovery does.
    pub fn recover_owner(&self, domain: &Eip712Domain) -> Result<Address, CapabilityError> {
        if crate::sig_canon::is_high_s(&self.signature) {
            return Err(CapabilityError::InvalidSignature);
        }
        let hash = self.capability.signing_hash(domain);
        self.signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| CapabilityError::InvalidSignature)
    }

    /// Verify the capability was signed by `expected` (the pool owner) for
    /// `domain`.
    ///
    /// # Errors
    ///
    /// - [`CapabilityError::InvalidSignature`] — signature is malformed.
    /// - [`CapabilityError::WrongOwner`] — recovered address differs from
    ///   `expected`.
    pub fn verify_owner(
        &self,
        expected: Address,
        domain: &Eip712Domain,
    ) -> Result<(), CapabilityError> {
        let recovered = self.recover_owner(domain)?;
        if recovered == expected {
            Ok(())
        } else {
            Err(CapabilityError::WrongOwner {
                expected,
                recovered,
            })
        }
    }
}

/// Failure modes for capability verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CapabilityError {
    /// The signature is malformed — non-canonical `s`, invalid recovery id, or
    /// a corrupted byte. The capability is unusable as-is.
    #[error("capability signature is malformed")]
    InvalidSignature,
    /// The signature is well-formed but recovers to an address that does not
    /// match the expected pool owner.
    #[error("capability signed by {recovered}, expected owner {expected}")]
    WrongOwner {
        expected: Address,
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests {
    use super::*;
    use alloy::primitives::{address, b256, keccak256};
    use alloy::signers::local::PrivateKeySigner;

    fn sample_capability() -> Capability {
        Capability {
            signer: address!("00000000000000000000000000000000000000a1"),
            spending_cap: U256::from(10_000_000u64),
            pool_id: b256!("11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"),
            expiry: 1_900_000_000,
        }
    }

    fn sample_domain() -> Eip712Domain {
        voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
        r.err()
            .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
    }

    #[test]
    fn sign_then_recover_is_the_owner() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain = sample_domain();
        let signed = sample_capability().sign(&owner, &domain)?;
        let recovered = signed.recover_owner(&domain)?;
        anyhow::ensure!(recovered == owner.address(), "must recover the owner");
        signed.verify_owner(owner.address(), &domain)?;
        Ok(())
    }

    #[test]
    fn wrong_owner_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let other = PrivateKeySigner::random().address();
        let domain = sample_domain();
        let signed = sample_capability().sign(&owner, &domain)?;
        let err = err_of(signed.verify_owner(other, &domain))?;
        anyhow::ensure!(
            matches!(err, CapabilityError::WrongOwner { .. }),
            "expected WrongOwner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_signer_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_capability().sign(&owner, &domain)?;
        signed.capability.signer = address!("000000000000000000000000000000000000dead");
        let err = err_of(signed.verify_owner(owner.address(), &domain))?;
        anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn tampered_spending_cap_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_capability().sign(&owner, &domain)?;
        signed.capability.spending_cap += U256::from(1u64);
        let err = err_of(signed.verify_owner(owner.address(), &domain))?;
        anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn tampered_pool_id_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_capability().sign(&owner, &domain)?;
        signed.capability.pool_id = B256::ZERO;
        let err = err_of(signed.verify_owner(owner.address(), &domain))?;
        anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn tampered_expiry_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain = sample_domain();
        let mut signed = sample_capability().sign(&owner, &domain)?;
        signed.capability.expiry += 1;
        let err = err_of(signed.verify_owner(owner.address(), &domain))?;
        anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn high_s_capability_signature_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain = sample_domain();
        let signed = sample_capability().sign(&owner, &domain)?;
        let twin = SignedCapability {
            signature: crate::sig_canon::high_s_twin(&signed.signature),
            ..signed
        };
        anyhow::ensure!(
            twin.recover_owner(&domain) == Err(CapabilityError::InvalidSignature),
            "high-s capability twin must be rejected"
        );
        Ok(())
    }

    #[test]
    fn different_chain_id_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain_a = sample_domain();
        let domain_b = voucher_domain(1, address!("0000000000000000000000000000000000001234"));
        let signed = sample_capability().sign(&owner, &domain_a)?;
        let err = err_of(signed.verify_owner(owner.address(), &domain_b))?;
        anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
        Ok(())
    }

    #[test]
    fn different_verifying_contract_rejected() -> anyhow::Result<()> {
        let owner = PrivateKeySigner::random();
        let domain_a = sample_domain();
        let domain_b = voucher_domain(
            421_614,
            address!("0000000000000000000000000000000000005678"),
        );
        let signed = sample_capability().sign(&owner, &domain_a)?;
        let err = err_of(signed.verify_owner(owner.address(), &domain_b))?;
        anyhow::ensure!(matches!(err, CapabilityError::WrongOwner { .. }), "{err:?}");
        Ok(())
    }

    /// Lock the EIP-712 type hash to the exact `PaymentPool.CAPABILITY_TYPEHASH`
    /// wording. If this breaks, the contract typehash or the `sol!` canonical
    /// encoding drifted — a coordinated update with the Solidity contract.
    #[test]
    fn capability_type_hash_matches_adr_003() -> anyhow::Result<()> {
        let canonical: &[u8] =
            b"Capability(address signer,uint256 spendingCap,bytes32 poolId,uint64 expiry)";
        let expected = keccak256(canonical);
        let actual = CapabilitySol::eip712_type_hash(&sample_capability().to_sol());
        anyhow::ensure!(
            actual == expected,
            "capability type hash drifted: actual={actual} expected={expected}"
        );
        Ok(())
    }

    /// Fixed-vector regression: a deterministic key + capability must recover
    /// the owner's address. Catches a Rust-side digest drift before the
    /// contract does.
    #[test]
    fn fixed_vector_recovers_expected_owner() -> anyhow::Result<()> {
        let pk_hex = "1111111111111111111111111111111111111111111111111111111111111111";
        let owner: PrivateKeySigner = pk_hex
            .parse()
            .map_err(|e| anyhow::anyhow!("parse test key: {e}"))?;
        let address = owner.address();
        let signed = sample_capability().sign(&owner, &sample_domain())?;
        let recovered = signed.recover_owner(&sample_domain())?;
        anyhow::ensure!(
            recovered == address,
            "recovered {recovered}, expected {address}"
        );
        Ok(())
    }

    /// A capability and a voucher over overlapping fields must produce
    /// *different* digests — the distinct type-string is what stops one
    /// standing in for the other.
    #[test]
    fn capability_digest_differs_from_voucher_digest() -> anyhow::Result<()> {
        use crate::voucher::Voucher;
        let domain = sample_domain();
        let cap = sample_capability();
        let voucher = Voucher {
            pool_id: cap.pool_id,
            signer: cap.signer,
            provider: address!("00000000000000000000000000000000000000b2"),
            amount: cap.spending_cap,
            bytes_delivered: U256::from(1u64),
        };
        anyhow::ensure!(
            cap.signing_hash(&domain) != voucher.signing_hash(&domain),
            "capability and voucher digests must differ (distinct typehash)"
        );
        Ok(())
    }
}
