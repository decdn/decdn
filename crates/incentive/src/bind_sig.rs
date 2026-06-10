//! EIP-712 `BindNodeId` verification for ephemeral client bindings.
//!
//! Clients without on-chain registration MAY attach a signed
//! `NodeId`↔Ethereum-address binding to their first `StreamRequest` on a
//! connection (ADR 003 §Off-Chain Ephemeral Binding, ADR 005 §Client identity
//! binding). The delivering node verifies the EIP-712 signature, recovers the
//! Ethereum address, and uses it for voucher attribution for the connection's
//! lifetime. The binding is not stored on-chain.
//!
//! The signed payload follows ADR 003 §Binding Message Format exactly so an
//! off-chain Rust verification byte-matches what the on-chain `CapacityBond`
//! contract accepts:
//!
//! ```text
//! BindNodeId(bytes32 nodeId,uint64 nonce)
//! ```
//!
//! Ephemeral client bindings always sign `nonce = 0` (the on-chain registration
//! path uses the per-address `bindingNonce`, a distinct domain of values).
//!
//! # Domain
//!
//! The EIP-712 domain is the `CapacityBond` contract deployment
//! (`EIP712("CapacityBond", "1")`, see `contracts/src/CapacityBond.sol`), so an
//! ephemeral binding is bound to the same chain + contract as on-chain
//! registration, preventing cross-chain / cross-contract replay (ADR 003
//! §Binding Message Format).
//!
//! # Scope
//!
//! Only EOA (`ecrecover`) verification is implemented here, matching the
//! EOA-only off-chain signing stance of [`crate::probe_sig`] and
//! [`crate::voucher`]. ERC-1271 smart-account bindings (ADR 024 §Off-Chain
//! ERC-1271 Verification) require an on-chain `isValidSignature` RPC call and
//! are deferred.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::sol_types::{SolStruct, eip712_domain};

/// EIP-712 domain `name` field for the `CapacityBond` deployment. Must match
/// `EIP712("CapacityBond", "1")` in `contracts/src/CapacityBond.sol` — a
/// mismatch produces a different digest and every binding fails to recover the
/// expected address.
pub const CAPACITY_BOND_DOMAIN_NAME: &str = "CapacityBond";

/// EIP-712 domain `version` field.
pub const CAPACITY_BOND_DOMAIN_VERSION: &str = "1";

/// `nonce` value for ephemeral (off-chain) client bindings (ADR 003 §Off-Chain
/// Ephemeral Binding). On-chain registration uses the per-address
/// `bindingNonce` instead.
pub const EPHEMERAL_BINDING_NONCE: u64 = 0;

// Solidity struct mirroring ADR 003 §Binding Message Format. The `sol!` macro
// uses the Rust struct name as the on-chain Solidity type name; the contract's
// struct is `BindNodeId`, wrapped in a private module to keep the name local.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct BindNodeId {
            bytes32 nodeId;
            uint64 nonce;
        }
    }
}

use sol_types::BindNodeId as BindNodeIdSol;

/// Construct the EIP-712 domain used to verify `BindNodeId` signatures for a
/// given `CapacityBond` deployment.
#[must_use]
pub fn bind_node_id_domain(chain_id: u64, verifying_contract: Address) -> Eip712Domain {
    eip712_domain! {
        name: CAPACITY_BOND_DOMAIN_NAME,
        version: CAPACITY_BOND_DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    }
}

/// EIP-712 signing hash for a `BindNodeId(nodeId, nonce)` under `domain`.
#[must_use]
pub fn binding_signing_hash(node_id: B256, nonce: u64, domain: &Eip712Domain) -> B256 {
    BindNodeIdSol {
        nodeId: node_id,
        nonce,
    }
    .eip712_signing_hash(domain)
}

/// Verify an ephemeral client binding and recover the attesting Ethereum
/// address (EOA `ecrecover`).
///
/// `node_id` is the client's 32-byte iroh `NodeId` (the connection's
/// authenticated remote id); `nonce` is [`EPHEMERAL_BINDING_NONCE`] for
/// off-chain bindings. The recovered address is what the node uses for voucher
/// attribution; the caller MUST further check it equals the channel's `client`
/// before accepting vouchers.
///
/// # Errors
///
/// - [`BindError::BadLength`] — `signature` is not 65 bytes (the EOA form).
/// - [`BindError::Malformed`] — well-formed length but non-canonical `s` /
///   invalid recovery id, so no address can be recovered.
pub fn verify_binding(
    node_id: B256,
    nonce: u64,
    signature: &[u8],
    domain: &Eip712Domain,
) -> Result<Address, BindError> {
    // EOA-only: a fixed 65-byte (r‖s‖v) signature. ERC-1271 (variable length)
    // is deferred (see module docs).
    if signature.len() != 65 {
        return Err(BindError::BadLength {
            len: signature.len(),
        });
    }
    let sig = Signature::from_raw(signature).map_err(|_| BindError::Malformed)?;
    // Reject non-canonical high-`s` so the off-chain accept-set matches the
    // on-chain verifiable-set (#836).
    if crate::sig_canon::is_high_s(&sig) {
        return Err(BindError::Malformed);
    }
    let hash = binding_signing_hash(node_id, nonce, domain);
    sig.recover_address_from_prehash(&hash)
        .map_err(|_| BindError::Malformed)
}

/// Failure modes for [`verify_binding`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BindError {
    /// Signature is not the 65-byte EOA form. ERC-1271 smart-account bindings
    /// (variable length) are not yet supported off-chain (ADR 024).
    #[error("binding signature has invalid length {len} (EOA form is 65 bytes)")]
    BadLength { len: usize },
    /// Signature is 65 bytes but malformed — non-canonical `s` or invalid
    /// recovery id; no address could be recovered.
    #[error("binding signature is malformed")]
    Malformed,
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests {
    use super::*;
    use alloy::primitives::address;
    use alloy::signers::SignerSync;
    use alloy::signers::local::PrivateKeySigner;

    fn sample_domain() -> Eip712Domain {
        bind_node_id_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    fn sign_binding(
        signer: &PrivateKeySigner,
        node_id: B256,
        nonce: u64,
        domain: &Eip712Domain,
    ) -> anyhow::Result<Vec<u8>> {
        let hash = binding_signing_hash(node_id, nonce, domain);
        Ok(signer.sign_hash_sync(&hash)?.as_bytes().to_vec())
    }

    #[test]
    fn round_trip_recovers_signer() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let node_id = B256::repeat_byte(0xAB);

        let sig = sign_binding(&signer, node_id, EPHEMERAL_BINDING_NONCE, &domain)?;
        let recovered = verify_binding(node_id, EPHEMERAL_BINDING_NONCE, &sig, &domain)?;
        anyhow::ensure!(recovered == signer.address(), "recovered {recovered}");
        Ok(())
    }

    #[test]
    fn high_s_binding_signature_rejected() -> anyhow::Result<()> {
        // The high-`s` twin of a valid binding recovers the same signer
        // off-chain but reverts on-chain; reject it to keep the accept-set
        // aligned with the on-chain verifiable-set (#836).
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let node_id = B256::repeat_byte(0xAB);
        let hash = binding_signing_hash(node_id, EPHEMERAL_BINDING_NONCE, &domain);
        let sig = signer.sign_hash_sync(&hash)?;
        verify_binding(node_id, EPHEMERAL_BINDING_NONCE, &sig.as_bytes(), &domain)?; // low-s ok

        let twin = crate::sig_canon::high_s_twin(&sig).as_bytes().to_vec();
        anyhow::ensure!(
            verify_binding(node_id, EPHEMERAL_BINDING_NONCE, &twin, &domain)
                == Err(BindError::Malformed),
            "high-s binding twin must be rejected as Malformed"
        );
        Ok(())
    }

    #[test]
    fn tampered_node_id_recovers_different_address() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let sig = sign_binding(
            &signer,
            B256::repeat_byte(0x01),
            EPHEMERAL_BINDING_NONCE,
            &domain,
        )?;
        // Verify against a different node_id — recovers some other address, not
        // the real signer (the digest changed).
        let recovered = verify_binding(
            B256::repeat_byte(0x02),
            EPHEMERAL_BINDING_NONCE,
            &sig,
            &domain,
        )?;
        anyhow::ensure!(recovered != signer.address(), "tamper must change recovery");
        Ok(())
    }

    #[test]
    fn tampered_nonce_recovers_different_address() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let node_id = B256::repeat_byte(0xAB);
        let sig = sign_binding(&signer, node_id, EPHEMERAL_BINDING_NONCE, &domain)?;
        let recovered = verify_binding(node_id, 1, &sig, &domain)?;
        anyhow::ensure!(
            recovered != signer.address(),
            "nonce change must alter recovery"
        );
        Ok(())
    }

    #[test]
    fn wrong_length_rejected() -> anyhow::Result<()> {
        let domain = sample_domain();
        let err = verify_binding(B256::ZERO, 0, &[0u8; 64], &domain)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected BadLength, got Ok"))?;
        anyhow::ensure!(matches!(err, BindError::BadLength { len: 64 }), "{err:?}");
        Ok(())
    }

    #[test]
    fn type_hash_matches_adr_003() {
        use alloy::primitives::keccak256;
        let canonical: &[u8] = b"BindNodeId(bytes32 nodeId,uint64 nonce)";
        let expected = keccak256(canonical);
        let actual = BindNodeIdSol::eip712_type_hash(&BindNodeIdSol {
            nodeId: B256::ZERO,
            nonce: 0,
        });
        assert_eq!(actual, expected, "BindNodeId type hash drifted");
    }
}
