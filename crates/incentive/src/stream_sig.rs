//! EIP-712 `slash_sig` signatures for `cdn/client/v1` `StreamResponse` messages.
//!
//! Every `StreamResponse` carries a mandatory, non-empty secp256k1 EIP-712
//! signature (`slash_sig`) over its frozen signed field set. The signature
//! makes the response cryptographically attributable to a registered node and
//! is the on-chain evidence for rate-manipulation and blacklist-violation
//! slashing (ADR 005 §`cdn/client/v1`, ADR 014). This mirrors
//! [`crate::probe_sig`] for the delivery protocol. A signed refusal
//! (`ok:false`) is inert as evidence — rate manipulation requires `ok:true`
//! (ADR 014 § Rate manipulation), so a node may sign refusals freely.
//!
//! The signed payload follows ADR 014 §EIP-712 Type Definitions exactly so an
//! off-chain Rust signature byte-matches what the on-chain `SlashJudge`
//! contract recovers signatures against. `hash` and `pool_id` are
//! request-context fields (from the `StreamRequest`); in this implementation's
//! wire shape they are echoed back in
//! `decdn_protocol::StreamResponseBody.{hash,pool_id}` and are part of the
//! EIP-712 signed set: a verifier reconstructs the typed data from the
//! response body's own fields.
//!
//! # Domain
//!
//! Shares the `SlashJudge` domain with [`crate::probe_sig`]
//! ([`crate::slash_judge_domain`]) — both `ProbeResponse` and `StreamResponse`
//! evidence are verified by the same contract, which distinguishes them by the
//! EIP-712 struct type, not by domain.
//!
//! # `StreamResponse` type
//!
//! ```text
//! StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,
//!                bytes32 poolId,uint64 timestampUs)
//! ```

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Signature};
use alloy::signers::SignerSync;
use alloy::sol_types::SolStruct;

use decdn_protocol::StreamResponseBody;

// Solidity struct mirroring ADR 014 §EIP-712 Type Definitions. Field names and
// order are part of the signed type — both must match the on-chain contract
// verbatim, hence the camelCase. The `sol!` macro generates a `SolStruct` impl
// whose `eip712_signing_hash` produces the same 32-byte digest the contract
// recovers signatures against.
//
// Wrapped in a private module because the `sol!` macro uses the Rust struct
// name as the on-chain Solidity type name in the EIP-712 type-string. The
// contract's struct is `StreamResponse`, so the Rust struct must be
// `StreamResponse` too — the wrapping module avoids a name clash with
// `decdn_protocol::StreamResponse`.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct StreamResponse {
            bytes32 hash;
            bool ok;
            uint64 ratePerMb;
            uint64 totalBytes;
            bytes32 poolId;
            uint64 timestampUs;
        }
    }
}

use sol_types::StreamResponse as StreamResponseSol;

/// Pinned keccak256 digest of the canonical `StreamResponse` EIP-712 type
/// string (ADR 014 §EIP-712 Type Definitions):
///
/// ```text
/// StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,bytes32 poolId,uint64 timestampUs)
/// ```
///
/// The Solidity suite pins `SlashJudge.STREAM_RESPONSE_TYPEHASH` to the same
/// digest (`contracts/test/SlashJudge.t.sol`), so a one-sided edit of either
/// side's type string fails default CI on both sides (#1843).
pub const STREAM_RESPONSE_TYPEHASH: B256 =
    alloy::primitives::b256!("0xc6d9e65c3527b52698988cae3aea61d5d8df64b415de933cc7e9762bc600eb3e");

/// The signed field set of a `StreamResponse` (ADR 014 §1).
///
/// `hash` and `pool_id` are request-context fields echoed back in the
/// response body; `timestamp_us` is the requester-generated timestamp echoed
/// from the `StreamRequest`. Together with `ok`, `rate_per_mb`, and
/// `total_bytes`, these are exactly the fields the on-chain `SlashJudge`
/// reconstructs to verify rate-manipulation and blacklist-violation evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSlashData {
    /// BLAKE3 hash of the delivered blob (iroh `Hash` bytes).
    pub hash: B256,
    /// Whether the node committed to serving the blob.
    pub ok: bool,
    /// The node's quoted rate in token base units per MB.
    pub rate_per_mb: u64,
    /// Total blob size in bytes.
    pub total_bytes: u64,
    /// The shared payment pool this response is bound to.
    pub pool_id: B256,
    /// Requester-generated microsecond timestamp echoed from the request.
    pub timestamp_us: u64,
}

impl StreamSlashData {
    /// Build the signed-field view from a wire [`StreamResponseBody`].
    #[must_use]
    pub fn from_response_body(body: &StreamResponseBody) -> Self {
        Self {
            hash: B256::from(body.hash),
            ok: body.ok,
            rate_per_mb: body.rate_per_mb,
            total_bytes: body.total_bytes,
            pool_id: B256::from(body.pool_id),
            timestamp_us: body.timestamp_us,
        }
    }

    const fn to_sol(self) -> StreamResponseSol {
        StreamResponseSol {
            hash: self.hash,
            ok: self.ok,
            ratePerMb: self.rate_per_mb,
            totalBytes: self.total_bytes,
            poolId: self.pool_id,
            timestampUs: self.timestamp_us,
        }
    }

    /// EIP-712 signing hash bound to `domain`. This is the 32-byte digest
    /// passed into `ecrecover` on-chain; it depends only on the data and
    /// `domain` and is independent of the signing key.
    #[must_use]
    pub fn signing_hash(&self, domain: &Eip712Domain) -> B256 {
        (*self).to_sol().eip712_signing_hash(domain)
    }

    /// EIP-712 struct hash (`keccak256(abi.encode(STREAM_RESPONSE_TYPEHASH,
    /// fields...))`), independent of any domain — the per-message hash
    /// `SlashJudge` folds into its `evidenceHash` commit (#1032, G-NODE-05).
    #[must_use]
    pub fn struct_hash(&self) -> B256 {
        (*self).to_sol().eip712_hash_struct()
    }

    /// Sign the stream response with `signer` for the given EIP-712 `domain`,
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
    /// Returns [`StreamSlashError::InvalidSignature`] if the signature is
    /// malformed (non-canonical `s`, invalid recovery id, etc.).
    pub fn recover_signer(
        &self,
        signature: &Signature,
        domain: &Eip712Domain,
    ) -> Result<Address, StreamSlashError> {
        // Reject non-canonical high-`s` so the off-chain accept-set matches the
        // on-chain `SlashJudge` verifiable-set (#836).
        if crate::sig_canon::is_high_s(signature) {
            return Err(StreamSlashError::InvalidSignature);
        }
        let hash = self.signing_hash(domain);
        signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| StreamSlashError::InvalidSignature)
    }

    /// Verify `signature` was produced by `expected` for `domain`.
    ///
    /// # Errors
    ///
    /// - [`StreamSlashError::InvalidSignature`] — signature is malformed.
    /// - [`StreamSlashError::WrongSigner`] — recovered address differs from
    ///   `expected`.
    pub fn verify_signer(
        &self,
        signature: &Signature,
        expected: Address,
        domain: &Eip712Domain,
    ) -> Result<(), StreamSlashError> {
        let recovered = self.recover_signer(signature, domain)?;
        if recovered == expected {
            Ok(())
        } else {
            Err(StreamSlashError::WrongSigner {
                expected,
                recovered,
            })
        }
    }
}

/// Failure modes for stream-response signature verification.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StreamSlashError {
    /// The signature is malformed — non-canonical `s`, invalid recovery id, or
    /// a corrupted byte. (Length is enforced earlier at the protocol boundary
    /// via `decdn_protocol::SLASH_SIG_LEN` before a `Signature` is ever
    /// constructed; this variant does not itself length-check.)
    #[error("stream slash_sig is malformed")]
    InvalidSignature,
    /// The signature is well-formed but recovers to an address that does not
    /// match the expected signer.
    #[error("stream slash_sig signed by {recovered}, expected {expected}")]
    WrongSigner {
        /// The address the stream response had to be signed by.
        expected: Address,
        /// The address the signature actually recovers to.
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed and address/addr pair up clearly here
mod tests {
    use super::*;
    use crate::slash_judge_domain;
    use alloy::primitives::address;
    use alloy::signers::local::PrivateKeySigner;

    fn sample_data() -> StreamSlashData {
        StreamSlashData {
            hash: B256::repeat_byte(0x7A),
            ok: true,
            rate_per_mb: 10_000,
            total_bytes: 1_048_576,
            pool_id: B256::repeat_byte(0x33),
            timestamp_us: 1_700_000_000_000_000,
        }
    }

    fn sample_domain() -> Eip712Domain {
        // Arbitrum Sepolia chain id; address is a deterministic test fixture.
        slash_judge_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        )
    }

    fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
        r.err()
            .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
    }

    #[test]
    fn round_trip_sign_verify() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        data.verify_signer(&sig, address, &domain)?;
        Ok(())
    }

    #[test]
    fn high_s_stream_slash_signature_rejected() -> anyhow::Result<()> {
        // High-`s` slash evidence verifies off-chain but fails at the on-chain
        // `SlashJudge`; reject it so collected evidence stays settleable (#836).
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();
        let sig = data.sign(&signer, &domain)?;
        data.verify_signer(&sig, signer.address(), &domain)?; // low-s ok

        let twin = crate::sig_canon::high_s_twin(&sig);
        anyhow::ensure!(
            data.recover_signer(&twin, &domain) == Err(StreamSlashError::InvalidSignature),
            "high-s stream slash twin must be rejected as InvalidSignature"
        );
        Ok(())
    }

    #[test]
    fn wrong_signer_address_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let other = PrivateKeySigner::random().address();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        let err = err_of(data.verify_signer(&sig, other, &domain))?;
        anyhow::ensure!(
            matches!(err, StreamSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn different_chain_id_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain_a = slash_judge_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        );
        let domain_b = slash_judge_domain(1, address!("0000000000000000000000000000000000001234"));
        let data = sample_data();

        let sig = data.sign(&signer, &domain_a)?;
        let err = err_of(data.verify_signer(&sig, signer.address(), &domain_b))?;
        anyhow::ensure!(
            matches!(err, StreamSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn different_verifying_contract_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain_a = slash_judge_domain(
            421_614,
            address!("0000000000000000000000000000000000001234"),
        );
        let domain_b = slash_judge_domain(
            421_614,
            address!("0000000000000000000000000000000000005678"),
        );
        let data = sample_data();

        let sig = data.sign(&signer, &domain_a)?;
        let err = err_of(data.verify_signer(&sig, signer.address(), &domain_b))?;
        anyhow::ensure!(
            matches!(err, StreamSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    /// Each signed field must be covered: tampering any one must flip the
    /// recovered signer.
    #[test]
    fn tampered_fields_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();
        let sig = data.sign(&signer, &domain)?;

        let mutations: [StreamSlashData; 6] = [
            StreamSlashData {
                hash: B256::ZERO,
                ..data
            },
            StreamSlashData { ok: false, ..data },
            StreamSlashData {
                rate_per_mb: data.rate_per_mb + 1,
                ..data
            },
            StreamSlashData {
                total_bytes: data.total_bytes + 1,
                ..data
            },
            StreamSlashData {
                pool_id: B256::ZERO,
                ..data
            },
            StreamSlashData {
                timestamp_us: data.timestamp_us + 1,
                ..data
            },
        ];
        for (i, m) in mutations.into_iter().enumerate() {
            let err = err_of(m.verify_signer(&sig, signer.address(), &domain))?;
            anyhow::ensure!(
                matches!(err, StreamSlashError::WrongSigner { .. }),
                "mutation {i} should flip the signer, got {err:?}"
            );
        }
        Ok(())
    }

    /// Lock the EIP-712 type hash to the exact ADR 014 wording and to the
    /// pinned [`STREAM_RESPONSE_TYPEHASH`] digest the Solidity suite also
    /// asserts. If this breaks, either the ADR changed or the `sol!` macro's
    /// canonical encoding shifted — both warrant a coordinated update with the
    /// `SlashJudge` contract and its deployment manifests.
    #[test]
    fn stream_response_type_hash_matches_adr_014() -> anyhow::Result<()> {
        use alloy::primitives::keccak256;
        let canonical: &[u8] = b"StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,bytes32 poolId,uint64 timestampUs)";
        anyhow::ensure!(
            keccak256(canonical) == STREAM_RESPONSE_TYPEHASH,
            "pinned STREAM_RESPONSE_TYPEHASH does not match the ADR 014 type string"
        );
        let actual = StreamResponseSol::eip712_type_hash(&sample_data().to_sol());
        anyhow::ensure!(
            actual == STREAM_RESPONSE_TYPEHASH,
            "StreamResponse type hash drifted: actual={actual} expected={STREAM_RESPONSE_TYPEHASH}"
        );
        Ok(())
    }

    /// Independently reconstruct the EIP-712 signing digest from the canonical
    /// `0x1901 || domainSeparator || hashStruct` preimage and pin it against
    /// `signing_hash`. Unlike a self-consistent sign→recover, this proves the
    /// struct field encoding and domain binding match the on-chain `SlashJudge`
    /// computation — a real drift guard, not a tautology.
    #[test]
    fn signing_hash_matches_eip712_canonical() -> anyhow::Result<()> {
        use alloy::primitives::keccak256;
        use alloy::sol_types::SolValue;

        let data = sample_data();
        let domain = sample_domain();

        // EIP-712 encodeData: each field as a 32-byte ABI word, prefixed by the
        // pinned type hash. All fields here are static, so abi_encode of the
        // tuple is exactly 7 × 32 bytes.
        let struct_hash = keccak256(
            (
                STREAM_RESPONSE_TYPEHASH,
                data.hash,
                data.ok,
                data.rate_per_mb,
                data.total_bytes,
                data.pool_id,
                data.timestamp_us,
            )
                .abi_encode(),
        );
        let mut preimage = Vec::with_capacity(2 + 32 + 32);
        preimage.extend_from_slice(&[0x19, 0x01]);
        preimage.extend_from_slice(domain.separator().as_slice());
        preimage.extend_from_slice(struct_hash.as_slice());
        let expected = keccak256(&preimage);

        anyhow::ensure!(
            data.signing_hash(&domain) == expected,
            "signing_hash drifted from canonical EIP-712: actual={} expected={expected}",
            data.signing_hash(&domain)
        );
        Ok(())
    }

    /// `from_response_body` maps the wire body into the signed-field view.
    #[test]
    fn from_response_body_maps_fields() {
        use decdn_protocol::StreamResponseBody;

        let body = StreamResponseBody {
            hash: [0x7Au8; 32],
            ok: true,
            rate_per_mb: 10_000,
            total_bytes: 1_048_576,
            pool_id: [0x33u8; 32],
            timestamp_us: 1_700_000_000_000_000,
        };
        assert_eq!(StreamSlashData::from_response_body(&body), sample_data());
    }
}
