//! EIP-712 payment vouchers for the `PaymentPool` contract.
//!
//! Vouchers are off-chain signed messages a client (payer) issues to a node
//! (provider) as content is delivered. Each voucher carries a cumulative
//! `amount` of `USDC` base units (`µUSDC`) and a cumulative `bytes_delivered`
//! count, scoped to a `(pool_id, signer, provider)` lane. The delivering node
//! holds the latest voucher and redeems it on-chain against the pool.
//!
//! The signed payload follows ADR 003 §EIP-712 Voucher Signature exactly so
//! that an off-chain Rust signature byte-matches what the on-chain
//! `PaymentPool.redeem` accepts.
//!
//! # Domain
//!
//! ```text
//! EIP712Domain {
//!     name: "PaymentPool",
//!     version: "1",
//!     chainId: <L2 chain id>,
//!     verifyingContract: <PaymentPool deployment address>,
//! }
//! ```
//!
//! # Voucher type
//!
//! ```text
//! Voucher(bytes32 poolId,address signer,address provider,
//!         uint256 amount,uint256 bytesDelivered,
//!         bytes32 chainRoot,uint256 chunkPrice)
//! ```
//!
//! There is no nonce: `amount` is the sole monotone ordering and replay key —
//! a voucher whose `amount` is no greater than the highest already accepted is
//! stale (ADR 003 §Voucher ordering).
//!
//! `amount` is the settlement anchor and `chain_root` heads the optional
//! `PayWord` hash chain that advances it between signatures, so redemption
//! resolves both as `claimed = amount + chain_index × chunk_price` (ADR 003
//! §Hash-chain metering (`PayWord`); the chain primitive lives in
//! [`crate::chain`]). Neither `chain_length` nor `chunk_bytes` is signed —
//! both are protocol constants, so signing them would pay calldata for values
//! every party already holds.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::{SolStruct, eip712_domain};

/// EIP-712 domain `name` field. Must match the `PaymentPool` contract's
/// domain exactly — a mismatch produces a different digest and every
/// signature fails on-chain.
pub const DOMAIN_NAME: &str = "PaymentPool";

/// EIP-712 domain `version` field.
pub const DOMAIN_VERSION: &str = "1";

// Solidity struct mirroring ADR 003 §EIP-712 Voucher Signature. Field names
// and order are part of the signed type — both must match the on-chain
// contract verbatim, hence the camelCase. The `sol!` macro generates a
// `SolStruct` impl whose `eip712_signing_hash` produces the same 32-byte
// digest the contract recovers signatures against.
//
// Wrapped in a private module because the `sol!` macro uses the Rust struct
// name as the on-chain Solidity type name in the EIP-712 type-string. The
// contract's struct is `Voucher`, so the Rust struct must be `Voucher` too —
// the wrapping module avoids the name clash with our public Rust [`Voucher`].
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct Voucher {
            bytes32 poolId;
            address signer;
            address provider;
            uint256 amount;
            uint256 bytesDelivered;
            bytes32 chainRoot;
            uint256 chunkPrice;
        }
    }
}

use sol_types::Voucher as VoucherSol;

/// Construct the EIP-712 domain used to sign vouchers for a given
/// `PaymentPool` deployment.
#[must_use]
pub fn voucher_domain(chain_id: u64, verifying_contract: Address) -> Eip712Domain {
    eip712_domain! {
        name: DOMAIN_NAME,
        version: DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    }
}

/// A payment voucher in its unsigned form.
///
/// All amounts are cumulative across the lane's lifetime — a voucher with
/// `amount = 100` does not mean "pay 100 more" but "the total claimable on
/// this lane is 100." `bytes_delivered` is cumulative likewise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voucher {
    /// The pool this voucher draws from — the on-chain `PaymentPool` deposit id.
    pub pool_id: B256,
    /// The address whose EIP-712 signature authorizes this voucher (the
    /// capability signer the pool owner delegated spend to).
    pub signer: Address,
    /// The provider node this voucher pays — a capability voucher is scoped to
    /// one provider and is invalid if redeemed against another.
    pub provider: Address,
    /// Cumulative payment in token base units (`µUSDC` for `USDC`). The
    /// settlement anchor a hash chain extends.
    pub amount: U256,
    /// Cumulative bytes delivered against this lane.
    pub bytes_delivered: U256,
    /// Head of the hash chain this voucher opens —
    /// [`crate::chain::root_from_seed`] of the payer's per-lane seed. Zero
    /// **seals** the voucher at exactly `amount`: nothing hashes to zero, so no
    /// index above 0 can redeem against it (ADR 003 §The sealed voucher).
    pub chain_root: B256,
    /// What one chunk of delivery adds over `amount`, in token base units.
    /// Signed so the claim arithmetic is fixed at signing time; the floor clamp
    /// at redemption still reads the live `deliveryFloor`. Zero on a sealed
    /// voucher, which meters no chunk.
    pub chunk_price: U256,
}

impl Voucher {
    const fn to_sol(&self) -> VoucherSol {
        VoucherSol {
            poolId: self.pool_id,
            signer: self.signer,
            provider: self.provider,
            amount: self.amount,
            bytesDelivered: self.bytes_delivered,
            chainRoot: self.chain_root,
            chunkPrice: self.chunk_price,
        }
    }

    /// EIP-712 signing hash bound to `domain`. This is the 32-byte digest
    /// passed into `ecrecover` on-chain — identical for any signer.
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        self.to_sol().eip712_signing_hash(domain)
    }

    /// Sign the voucher with `signer` for the given EIP-712 `domain`.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying signer (key locked,
    /// remote signer offline, etc.).
    pub fn sign<S: SignerSync>(
        self,
        signer: &S,
        domain: &Eip712Domain,
    ) -> Result<SignedVoucher, alloy::signers::Error> {
        let hash = self.signing_hash(domain);
        let signature = signer.sign_hash_sync(&hash)?;
        Ok(SignedVoucher {
            voucher: self,
            signature,
        })
    }
}

/// A voucher together with its EIP-712 signature.
///
/// Holding a `SignedVoucher` is sufficient for a node to redeem against the
/// pool — the signature recovers the capability signer's address, which the
/// contract then matches against the registered capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedVoucher {
    /// The cumulative payment claim.
    pub voucher: Voucher,
    /// The capability signer's EIP-712 signature over it.
    pub signature: Signature,
}

impl SignedVoucher {
    /// Recover the address that signed this voucher under `domain`.
    ///
    /// # Errors
    ///
    /// Returns [`VoucherError::InvalidSignature`] if the signature is
    /// malformed (non-canonical `s`, invalid recovery id, etc.).
    pub fn recover_signer(&self, domain: &Eip712Domain) -> Result<Address, VoucherError> {
        // Reject non-canonical high-`s` up front so the off-chain accept-set
        // matches the on-chain verifiable-set (#836); alloy would otherwise
        // silently normalize and accept it.
        if crate::sig_canon::is_high_s(&self.signature) {
            return Err(VoucherError::InvalidSignature);
        }
        let hash = self.voucher.signing_hash(domain);
        self.signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| VoucherError::InvalidSignature)
    }

    /// Verify the signature was produced by `expected` for `domain`.
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

/// Failure modes for voucher verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VoucherError {
    /// The signature is malformed — non-canonical `s`, invalid recovery id,
    /// or a corrupted byte. The voucher is unusable as-is.
    #[error("voucher signature is malformed")]
    InvalidSignature,
    /// The signature is well-formed but recovers to an address that does not
    /// match the expected signer — e.g., the lane's pinned capability signer.
    #[error("voucher signed by {recovered}, expected {expected}")]
    WrongSigner {
        /// The address the voucher had to be signed by.
        expected: Address,
        /// The address the signature actually recovers to.
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed and address/addr pair up clearly here
mod tests;

/// Property-based tests for the EIP-712 voucher core (#740). The example-based
/// `tests` module above pins fixed vectors and single-field tamper cases with
/// small values; these sweep the full `U256` keyspace for `amount` /
/// `bytes_delivered` with the dangerous boundaries (`0`, `u64::MAX`,
/// `U256::MAX`) heavily over-sampled (see [`any_u256`]), to catch any silent
/// truncation or accept-invalid path before mainnet. We trust `alloy` for the
/// EIP-712
/// primitive and assert the invariants the protocol depends on: sign→recover is
/// total over that range, the digest is deterministic and covers every field,
/// and the domain is binding.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod prop_tests;
