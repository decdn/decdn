//! EIP-712 signatures on origin attestations (ADR 022 § Origin Attestation).
//!
//! A seated origin signs an attestation with its operator key to state that it
//! serves a hash in a namespace until the attestation expires:
//!
//! ```text
//! OriginAttestation(bytes32 hash,uint256 namespaceId,uint64 timestampUs,uint32 ttlSec)
//! ```
//!
//! The signature is checked off-chain only, by the nodes that keep, serve and
//! publish the hash. It uses the `SlashJudge` EIP-712 domain
//! ([`crate::slash_judge_domain`]), which every node already holds and which
//! binds the signature to one chain and one deployment. Its type name differs
//! from `StreamResponse`, so neither signature verifies as the other.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::SolStruct;

use decdn_protocol::OriginAttestation;

use crate::StreamSlashError;

// Field names and order are part of the signed type.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations, missing_docs)]
        struct OriginAttestation {
            bytes32 hash;
            uint256 namespaceId;
            uint64 timestampUs;
            uint32 ttlSec;
        }
    }
}

/// Pinned keccak256 digest of the `OriginAttestation` EIP-712 type string.
pub const ORIGIN_ATTESTATION_TYPEHASH: B256 =
    alloy::primitives::b256!("0x445f943baab8eb4a394daaa61f4829fb63ac3bc9464fa5611c0362932561f821");

fn to_sol(hash: [u8; 32], attestation: &OriginAttestation) -> sol_types::OriginAttestation {
    sol_types::OriginAttestation {
        hash: B256::from(hash),
        namespaceId: U256::from_be_bytes(attestation.namespace_id),
        timestampUs: attestation.timestamp_us,
        ttlSec: attestation.ttl_sec,
    }
}

/// The EIP-712 signing hash of `attestation` for `hash` under `domain`.
#[must_use]
pub fn origin_attestation_signing_hash(
    hash: [u8; 32],
    attestation: &OriginAttestation,
    domain: &Eip712Domain,
) -> B256 {
    to_sol(hash, attestation).eip712_signing_hash(domain)
}

/// Sign an origin attestation for `hash` in `namespace_id`, stamped
/// `timestamp_us` with a lifetime of `ttl_sec`.
///
/// # Errors
///
/// Propagates a signer failure.
pub fn sign_origin_attestation<S: SignerSync>(
    signer: &S,
    domain: &Eip712Domain,
    hash: [u8; 32],
    namespace_id: [u8; 32],
    timestamp_us: u64,
    ttl_sec: u32,
) -> Result<OriginAttestation, alloy::signers::Error> {
    let mut attestation = OriginAttestation {
        namespace_id,
        timestamp_us,
        ttl_sec,
        sig: Vec::new(),
    };
    let sig =
        signer.sign_hash_sync(&origin_attestation_signing_hash(hash, &attestation, domain))?;
    attestation.sig = sig.as_bytes().to_vec();
    Ok(attestation)
}

/// Recover the operator that signed `attestation` for `hash`. The caller
/// decides validity: the seat, the lifetime bounds and liveness.
///
/// # Errors
///
/// [`StreamSlashError::InvalidSignature`] if the signature does not parse,
/// has a high `s`, or does not recover.
pub fn recover_origin_attestation_signer(
    hash: [u8; 32],
    attestation: &OriginAttestation,
    domain: &Eip712Domain,
) -> Result<Address, StreamSlashError> {
    let sig = Signature::try_from(attestation.sig.as_slice())
        .map_err(|_| StreamSlashError::InvalidSignature)?;
    if crate::sig_canon::is_high_s(&sig) {
        return Err(StreamSlashError::InvalidSignature);
    }
    sig.recover_address_from_prehash(&origin_attestation_signing_hash(hash, attestation, domain))
        .map_err(|_| StreamSlashError::InvalidSignature)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
