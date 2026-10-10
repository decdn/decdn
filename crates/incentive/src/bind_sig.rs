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
//! [`crate::voucher`]. Off-chain verification is EOA recovery only (ADR 024
//! §Off-Chain Signature Verification — EOA Recovery Only); a smart-account
//! client binds through a capability-delegated EOA `signer` instead, so no
//! ERC-1271 off-chain branch exists.

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
        // ADR 019 § Terms Acceptance — the registration binding signature
        // additionally commits to the accepted `termsHash`. Distinct typehash
        // from `BindNodeId`: rebinding (key rotation) stays on `BindNodeId` and
        // does not re-accept terms.
        #[allow(non_snake_case, missing_debug_implementations)]
        struct RegisterNode {
            bytes32 nodeId;
            uint64 nonce;
            bytes32 termsHash;
        }
    }
}

use sol_types::BindNodeId as BindNodeIdSol;
use sol_types::RegisterNode as RegisterNodeSol;

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
///
/// Used for the ephemeral client binding (ADR 003 §Off-Chain Ephemeral Binding)
/// and on-chain rebinding (`bindNodeId`, key rotation). Initial node
/// registration signs [`register_node_signing_hash`] instead.
#[must_use]
pub fn binding_signing_hash(node_id: B256, nonce: u64, domain: &Eip712Domain) -> B256 {
    BindNodeIdSol {
        nodeId: node_id,
        nonce,
    }
    .eip712_signing_hash(domain)
}

/// EIP-712 signing hash for a `RegisterNode(nodeId, nonce, termsHash)` under
/// `domain` — the initial on-chain node registration binding (ADR 019 § Terms
/// Acceptance). `terms_hash` MUST equal the contract's `currentTermsHash`, and
/// the signature cryptographically binds the operator's acceptance to it.
#[must_use]
pub fn register_node_signing_hash(
    node_id: B256,
    nonce: u64,
    terms_hash: B256,
    domain: &Eip712Domain,
) -> B256 {
    RegisterNodeSol {
        nodeId: node_id,
        nonce,
        termsHash: terms_hash,
    }
    .eip712_signing_hash(domain)
}

/// Verify an ephemeral client binding and recover the attesting Ethereum
/// address (EOA `ecrecover`).
///
/// `node_id` is the client's 32-byte iroh `NodeId` (the connection's
/// authenticated remote id); `nonce` is [`EPHEMERAL_BINDING_NONCE`] for
/// off-chain bindings. The recovered address is what the node uses for voucher
/// attribution; the caller MUST further check it equals the lane's `signer`
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
    /// (variable length) are not supported off-chain; a smart-account client
    /// binds through a capability-delegated EOA `signer` instead (ADR 024
    /// §Off-Chain Signature Verification — EOA Recovery Only).
    #[error("binding signature has invalid length {len} (EOA form is 65 bytes)")]
    BadLength {
        /// The length that was offered.
        len: usize,
    },
    /// Signature is 65 bytes but malformed — non-canonical `s` or invalid
    /// recovery id; no address could be recovered.
    #[error("binding signature is malformed")]
    Malformed,
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed pair up clearly here
mod tests;
