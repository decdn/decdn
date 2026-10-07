//! EIP-712 `slash_sig` signatures for `cdn/probe/v1` `ProbeResponse` messages.
//!
//! Every `ProbeResponse` carries a mandatory, non-empty secp256k1 EIP-712
//! signature (`slash_sig`) over its frozen signed field set. The signature
//! makes the response cryptographically attributable to a registered node
//! and is the on-chain evidence for rate-manipulation and blacklist-violation
//! slashing (ADR 005 §`cdn/probe/v1`, ADR 014).
//!
//! The signed payload follows ADR 014 §EIP-712 Type Definitions exactly so an
//! off-chain Rust signature byte-matches what the on-chain `SlashJudge`
//! contract recovers signatures against. `hash` is the queried blob hash:
//! in this implementation's wire shape it *is* echoed back in
//! `decdn_protocol::ProbeResponseBody.hash` (not omitted as request-only
//! context), and it is part of the EIP-712 signed set (ADR 014 §1) — so a
//! verifier reconstructs the typed data from the response body's own fields.
//!
//! # Domain
//!
//! ```text
//! EIP712Domain {
//!     name: "deCDN SlashJudge",
//!     version: "1",
//!     chainId: <L2 chain id>,
//!     verifyingContract: <SlashJudge deployment address>,
//! }
//! ```
//!
//! The `SlashJudge` contract uses its own domain separator (distinct from
//! `CapacityBond` / `PaymentPool`) to prevent cross-contract
//! signature replay (ADR 014 §EIP-712 Type Definitions).
//!
//! # `ProbeResponse` type
//!
//! ```text
//! ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)
//! ```

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::SignerSync;
use alloy::sol_types::{SolStruct, eip712_domain};

/// EIP-712 domain `name` field. Must match the `SlashJudge` contract's domain
/// exactly — a mismatch produces a different digest and every signature fails
/// on-chain.
pub const SLASH_JUDGE_DOMAIN_NAME: &str = "deCDN SlashJudge";

/// EIP-712 domain `version` field.
pub const SLASH_JUDGE_DOMAIN_VERSION: &str = "1";

// Solidity struct mirroring ADR 014 §EIP-712 Type Definitions. Field names and
// order are part of the signed type — both must match the on-chain contract
// verbatim, hence the camelCase. The `sol!` macro generates a `SolStruct`
// impl whose `eip712_signing_hash` produces the same 32-byte digest the
// contract recovers signatures against.
//
// Wrapped in a private module because the `sol!` macro uses the Rust struct
// name as the on-chain Solidity type name in the EIP-712 type-string. The
// contract's struct is `ProbeResponse`, so the Rust struct must be
// `ProbeResponse` too — the wrapping module avoids a name clash with
// `decdn_protocol::ProbeResponse`.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct ProbeResponse {
            bytes32 hash;
            bool hasBlob;
            uint64 ratePerMb;
            uint64 timestampUs;
        }
    }
}

use sol_types::ProbeResponse as ProbeResponseSol;

/// Pinned keccak256 digest of the canonical `ProbeResponse` EIP-712 type
/// string (ADR 014 §EIP-712 Type Definitions):
///
/// ```text
/// ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)
/// ```
///
/// The Solidity suite pins `SlashJudge.PROBE_RESPONSE_TYPEHASH` to the same
/// digest (`contracts/test/SlashJudge.t.sol`), so a one-sided edit of either
/// side's type string fails default CI on both sides (#1843).
pub const PROBE_RESPONSE_TYPEHASH: B256 =
    alloy::primitives::b256!("0x4638cb019b77f8677ac7015556f19e56f0be5a6a0d9392006a7fb5b8acd587e5");

/// Construct the EIP-712 domain used to sign probe responses for a given
/// `SlashJudge` deployment.
#[must_use]
pub fn slash_judge_domain(chain_id: u64, verifying_contract: Address) -> Eip712Domain {
    eip712_domain! {
        name: SLASH_JUDGE_DOMAIN_NAME,
        version: SLASH_JUDGE_DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    }
}

/// The signed field set of a `ProbeResponse` (ADR 014 §1).
///
/// `hash` is the queried blob hash echoed from the `ProbeRequest`;
/// `timestamp_us` is the requester-generated timestamp echoed back. Together
/// with `has_blob` and `rate_per_mb` these are exactly the fields the
/// on-chain `SlashJudge` reconstructs to verify rate-manipulation and
/// blacklist-violation evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeSlashData {
    /// BLAKE3 hash of the probed blob (iroh `Hash` bytes).
    pub hash: B256,
    /// Whether the node claimed to hold the blob.
    pub has_blob: bool,
    /// The node's quoted rate in token base units per MB.
    pub rate_per_mb: u64,
    /// Requester-generated microsecond timestamp echoed from the request.
    pub timestamp_us: u64,
}

impl ProbeSlashData {
    const fn to_sol(self) -> ProbeResponseSol {
        ProbeResponseSol {
            hash: self.hash,
            hasBlob: self.has_blob,
            ratePerMb: self.rate_per_mb,
            timestampUs: self.timestamp_us,
        }
    }

    /// EIP-712 signing hash bound to `domain`. This is the 32-byte digest
    /// passed into `ecrecover` on-chain; it depends only on the data and
    /// `domain` and is independent of the signing key (different signers
    /// over the same data/domain produce different signatures of this same
    /// digest).
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        (*self).to_sol().eip712_signing_hash(domain)
    }

    /// EIP-712 struct hash (`keccak256(abi.encode(PROBE_RESPONSE_TYPEHASH,
    /// fields...))`), independent of any domain. This is the per-message hash
    /// `SlashJudge` folds into its `evidenceHash` commit
    /// (`keccak256(abi.encode(offense, probeStructHash, streamStructHash))`), so
    /// a challenger reconstructs the commitment from it (#1032, G-NODE-05).
    #[must_use]
    pub fn struct_hash(&self) -> B256 {
        (*self).to_sol().eip712_hash_struct()
    }

    /// Sign the probe response with `signer` for the given EIP-712 `domain`,
    /// returning the 65-byte (`r‖s‖v`) signature for the wire `slash_sig`.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying signer (key locked,
    /// remote signer offline, etc.).
    pub fn sign<S: SignerSync>(
        &self,
        signer: &S,
        domain: &Eip712Domain,
    ) -> Result<Signature, alloy::signers::Error> {
        let hash = self.signing_hash(domain);
        signer.sign_hash_sync(&hash)
    }

    /// Recover the address that produced `signature` over this data under
    /// `domain`.
    ///
    /// # Errors
    ///
    /// Returns [`ProbeSlashError::InvalidSignature`] if the signature is
    /// malformed (non-canonical `s`, invalid recovery id, etc.).
    pub fn recover_signer(
        &self,
        signature: &Signature,
        domain: &Eip712Domain,
    ) -> Result<Address, ProbeSlashError> {
        // Reject non-canonical high-`s` so the off-chain accept-set matches the
        // on-chain `SlashJudge` verifiable-set (#836).
        if crate::sig_canon::is_high_s(signature) {
            return Err(ProbeSlashError::InvalidSignature);
        }
        let hash = self.signing_hash(domain);
        signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| ProbeSlashError::InvalidSignature)
    }

    /// Verify `signature` was produced by `expected` for `domain`.
    ///
    /// # Errors
    ///
    /// - [`ProbeSlashError::InvalidSignature`] — signature is malformed.
    /// - [`ProbeSlashError::WrongSigner`] — recovered address differs from
    ///   `expected`.
    pub fn verify_signer(
        &self,
        signature: &Signature,
        expected: Address,
        domain: &Eip712Domain,
    ) -> Result<(), ProbeSlashError> {
        let recovered = self.recover_signer(signature, domain)?;
        if recovered == expected {
            Ok(())
        } else {
            Err(ProbeSlashError::WrongSigner {
                expected,
                recovered,
            })
        }
    }
}

/// Failure modes for probe-response signature verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProbeSlashError {
    /// The signature is malformed — non-canonical `s`, invalid recovery id,
    /// or a corrupted byte. (Length is enforced earlier at the protocol
    /// boundary via `decdn_protocol::SLASH_SIG_LEN` before an
    /// `alloy::primitives::Signature` is ever constructed; this variant does
    /// not itself length-check.)
    #[error("probe slash_sig is malformed")]
    InvalidSignature,
    /// The signature is well-formed but recovers to an address that does not
    /// match the expected signer.
    #[error("probe slash_sig signed by {recovered}, expected {expected}")]
    WrongSigner {
        /// The address the probe attestation had to be signed by.
        expected: Address,
        /// The address the signature actually recovers to.
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed and address/addr pair up clearly here
mod tests;
