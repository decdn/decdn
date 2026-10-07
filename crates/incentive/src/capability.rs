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
    /// against `pool_id` under this grant. A `u64` to match the
    /// `PaymentPool.spendingCap` storage/calldata width exactly; the EIP-712
    /// type-string still hashes it as a `uint256` word (the signing hash widens
    /// it with `U256::from`).
    pub spending_cap: u64,
    /// The pool this capability draws from — the on-chain `PaymentPool`
    /// deposit id.
    pub pool_id: B256,
    /// Unix-seconds expiry. After it, vouchers under this capability are no
    /// longer redeemable and the node stops accepting them.
    pub expiry: u64,
}

impl Capability {
    fn to_sol(&self) -> CapabilitySol {
        CapabilitySol {
            signer: self.signer,
            // The contract hashes its `uint64 spendingCap` as a zero-padded
            // `uint256` word (the type-string says `uint256`), so widening the
            // `u64` here reproduces the contract's EIP-712 digest byte-for-byte.
            spendingCap: U256::from(self.spending_cap),
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
    /// The delegation itself.
    pub capability: Capability,
    /// The pool owner's EIP-712 signature over it.
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
        /// The pool owner the capability had to be signed by.
        expected: Address,
        /// The address the signature actually recovers to.
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests;
