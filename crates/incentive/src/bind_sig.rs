//! EIP-712 `BindNodeId` verification for ephemeral client bindings.
//!
//! Clients without on-chain registration MAY attach a signed
//! `NodeId`↔Ethereum-address binding to their first `StreamRequest` on a
//! connection (ADR 003 §Off-Chain Ephemeral Binding, ADR 005 §Client identity
//! binding). The delivering node verifies the EIP-712 signature, recovers the
//! Ethereum address, and uses it for voucher attribution for the connection's
//! lifetime. The binding is not stored on-chain.
//!
//! The signed payload follows ADR 003 §Binding Message Format:
//!
//! ```text
//! BindNodeId(bytes32 nodeId,uint64 nonce)
//! ```
//!
//! Ephemeral client bindings always sign `nonce = 0`.
//!
//! # Domain
//!
//! The EIP-712 domain names the `CapacityBond` deployment (name
//! `"CapacityBond"`, version `"1"`, its chain id and address), so a binding is
//! bound to one network deployment, preventing cross-chain replay (ADR 003
//! §Binding Message Format). The domain is an off-chain namespace: no contract
//! verifies these signatures.
//!
//! # Scope
//!
//! Only EOA (`ecrecover`) verification is implemented here, matching the
//! EOA-only off-chain signing stance of [`crate::stream_sig`] and
//! [`crate::voucher`]. Off-chain verification is EOA recovery only (ADR 024
//! §Off-Chain Signature Verification — EOA Recovery Only); a smart-account
//! client binds through a capability-delegated EOA `signer` instead, so no
//! ERC-1271 off-chain branch exists.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::sol_types::{SolStruct, eip712_domain};

/// EIP-712 domain `name` field for the `CapacityBond` deployment. Signer and
/// verifier must agree on it: a mismatch produces a different digest and every
/// signature fails to recover the expected address.
pub const CAPACITY_BOND_DOMAIN_NAME: &str = "CapacityBond";

/// EIP-712 domain `version` field.
pub const CAPACITY_BOND_DOMAIN_VERSION: &str = "1";

/// `nonce` value for ephemeral (off-chain) client bindings (ADR 003 §Off-Chain
/// Ephemeral Binding).
pub const EPHEMERAL_BINDING_NONCE: u64 = 0;

// Solidity struct mirroring ADR 003 §Binding Message Format. The `sol!` macro
// uses the Rust struct name as the Solidity type name, so it is wrapped in a
// private module to keep the name local.
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

/// Construct the `CapacityBond` EIP-712 domain for a given deployment.
///
/// Every off-chain operator and client signature shares it: `BindNodeId` here,
/// the `StreamResponse` signature ([`crate::stream_sig`]) and the
/// `OriginAttestation` ([`crate::origin_attestation`]). Each signs a distinct
/// type name, so no signature verifies as another type.
#[must_use]
pub fn capacity_bond_domain(chain_id: u64, verifying_contract: Address) -> Eip712Domain {
    eip712_domain! {
        name: CAPACITY_BOND_DOMAIN_NAME,
        version: CAPACITY_BOND_DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    }
}

/// EIP-712 signing hash for a `BindNodeId(nodeId, nonce)` under `domain`: the
/// ephemeral client binding (ADR 003 §Off-Chain Ephemeral Binding).
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
    // Reject non-canonical high-`s` so each binding has one canonical
    // signature (#836).
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
