//! EIP-712 `operator_sig` signatures for `cdn/client/v1` `StreamResponse` messages.
//!
//! Every `StreamResponse` carries a mandatory, non-empty secp256k1 EIP-712
//! signature (`operator_sig`) over its frozen signed field set. The signature
//! makes the response attributable to a registered node: the requester checks
//! that it recovers to the operator bound to the serving `NodeId` (ADR 005
//! § Signer binding). Pull receipts carry the signed `timestamp_us`: the
//! requester's timestamp, which the node echoes unchanged once it finds it
//! within its clock-skew bound.
//!
//! `hash` is a request-context field (from the `StreamRequest`); in this
//! implementation's wire shape it is echoed back in
//! `decdn_protocol::StreamResponseBody.hash` and is part of the EIP-712 signed
//! set: a verifier reconstructs the typed data from the response body's own
//! fields.
//!
//! # Domain
//!
//! ```text
//! EIP712Domain {
//!     name: "CapacityBond",
//!     version: "1",
//!     chainId: <governance chain id>,
//!     verifyingContract: <CapacityBond deployment address>,
//! }
//! ```
//!
//! The signature shares the `CapacityBond` domain
//! ([`crate::capacity_bond_domain`]) with the client-binding signatures, which
//! binds it to one chain and one deployment. The `StreamResponse` type name
//! differs from every other type signed under that domain, so no other
//! signature verifies as a stream response.
//!
//! # `StreamResponse` type
//!
//! ```text
//! StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 timestampUs)
//! ```
//!
//! The blob size is not signed: a response states it only in its size proof,
//! which the receiver verifies against `hash` (ADR 005 § Bounded byte ranges).

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::SignerSync;
use alloy::sol_types::SolStruct;

use decdn_protocol::StreamResponseBody;

// Solidity struct mirroring ADR 005 § Signer binding. Field names and order are
// part of the signed type, hence the camelCase. The `sol!` macro generates a
// `SolStruct` impl whose `eip712_signing_hash` produces the 32-byte digest a
// verifier recovers the signer from.
//
// Wrapped in a private module because the `sol!` macro uses the Rust struct
// name as the Solidity type name in the EIP-712 type-string. The type is
// `StreamResponse`, so the Rust struct must be `StreamResponse` too — the
// wrapping module avoids a name clash with `decdn_protocol::StreamResponse`.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct StreamResponse {
            bytes32 hash;
            bool ok;
            uint64 ratePerMb;
            uint64 timestampUs;
        }
    }
}

use sol_types::StreamResponse as StreamResponseSol;

/// Pinned keccak256 digest of the canonical `StreamResponse` EIP-712 type
/// string (ADR 005 § Signer binding):
///
/// ```text
/// StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 timestampUs)
/// ```
///
/// A test pins the `sol!` type string to this digest, so an edit of the type
/// string fails CI.
pub const STREAM_RESPONSE_TYPEHASH: B256 =
    alloy::primitives::b256!("0x8cdc58326f067436e492abcb22129dd2bcee462a10809d3ed3a0377858ebd535");

/// The signed field set of a `StreamResponse` (ADR 005 § Signer binding).
///
/// `hash` is a request-context field echoed back in the response body;
/// `timestamp_us` is the requester-generated timestamp echoed
/// from the `StreamRequest`. Together with `ok` and `rate_per_mb`, these are
/// exactly the fields a verifier reconstructs to recover the signer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSigData {
    /// BLAKE3 hash of the delivered blob (iroh `Hash` bytes).
    pub hash: B256,
    /// Whether the node committed to serving the blob.
    pub ok: bool,
    /// The node's quoted rate in token base units per MB.
    pub rate_per_mb: u64,
    /// Requester-generated microsecond timestamp echoed from the request.
    pub timestamp_us: u64,
}

impl StreamSigData {
    /// Build the signed-field view from a wire [`StreamResponseBody`].
    #[must_use]
    pub fn from_response_body(body: &StreamResponseBody) -> Self {
        Self {
            hash: B256::from(body.hash),
            ok: body.ok,
            rate_per_mb: body.rate_per_mb,
            timestamp_us: body.timestamp_us,
        }
    }

    const fn to_sol(self) -> StreamResponseSol {
        StreamResponseSol {
            hash: self.hash,
            ok: self.ok,
            ratePerMb: self.rate_per_mb,
            timestampUs: self.timestamp_us,
        }
    }

    /// EIP-712 signing hash bound to `domain`. This is the 32-byte digest a
    /// verifier recovers the signer from; it depends only on the data and
    /// `domain` and is independent of the signing key.
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        (*self).to_sol().eip712_signing_hash(domain)
    }

    /// Sign the stream response with `signer` for the given EIP-712 `domain`,
    /// returning the 65-byte (`r‖s‖v`) signature for the wire `operator_sig`.
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
    /// Returns [`StreamSigError::InvalidSignature`] if the signature is
    /// malformed (non-canonical `s`, invalid recovery id, etc.).
    pub fn recover_signer(
        &self,
        signature: &Signature,
        domain: &Eip712Domain,
    ) -> Result<Address, StreamSigError> {
        // Reject non-canonical high-`s`: each response has exactly one
        // accepted signature encoding (#836).
        if crate::sig_canon::is_high_s(signature) {
            return Err(StreamSigError::InvalidSignature);
        }
        let hash = self.signing_hash(domain);
        signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| StreamSigError::InvalidSignature)
    }

    /// Verify `signature` was produced by `expected` for `domain`.
    ///
    /// # Errors
    ///
    /// - [`StreamSigError::InvalidSignature`] — signature is malformed.
    /// - [`StreamSigError::WrongSigner`] — recovered address differs from
    ///   `expected`.
    pub fn verify_signer(
        &self,
        signature: &Signature,
        expected: Address,
        domain: &Eip712Domain,
    ) -> Result<(), StreamSigError> {
        let recovered = self.recover_signer(signature, domain)?;
        if recovered == expected {
            Ok(())
        } else {
            Err(StreamSigError::WrongSigner {
                expected,
                recovered,
            })
        }
    }
}

/// Failure modes for stream-response signature verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StreamSigError {
    /// The signature is malformed — non-canonical `s`, invalid recovery id, or
    /// a corrupted byte. (Length is enforced earlier at the protocol boundary
    /// via `decdn_protocol::OPERATOR_SIG_LEN` before a `Signature` is ever
    /// constructed; this variant does not itself length-check.)
    #[error("stream operator_sig is malformed")]
    InvalidSignature,
    /// The signature is well-formed but recovers to an address that does not
    /// match the expected signer.
    #[error("stream operator_sig signed by {recovered}, expected {expected}")]
    WrongSigner {
        /// The address the stream response had to be signed by.
        expected: Address,
        /// The address the signature actually recovers to.
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed and address/addr pair up clearly here
mod tests;
