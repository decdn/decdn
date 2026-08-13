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
        expected: Address,
        recovered: Address,
    },
}

#[cfg(test)]
#[allow(clippy::similar_names)] // signer/signed and address/addr pair up clearly here
mod tests {
    use super::*;
    use alloy::primitives::address;
    use alloy::signers::local::PrivateKeySigner;

    fn sample_data() -> ProbeSlashData {
        ProbeSlashData {
            hash: B256::repeat_byte(0x7A),
            has_blob: true,
            rate_per_mb: 10_000,
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
    fn wrong_signer_address_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let other = PrivateKeySigner::random().address();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        let err = err_of(data.verify_signer(&sig, other, &domain))?;
        anyhow::ensure!(
            matches!(err, ProbeSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn high_s_probe_slash_signature_rejected() -> anyhow::Result<()> {
        // High-`s` slash evidence verifies off-chain but fails at the on-chain
        // `SlashJudge`; reject it so collected evidence stays settleable (#836).
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();
        let sig = data.sign(&signer, &domain)?;
        data.verify_signer(&sig, signer.address(), &domain)?; // low-s ok

        let twin = crate::sig_canon::high_s_twin(&sig);
        anyhow::ensure!(
            data.recover_signer(&twin, &domain) == Err(ProbeSlashError::InvalidSignature),
            "high-s probe slash twin must be rejected as InvalidSignature"
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
            matches!(err, ProbeSlashError::WrongSigner { .. }),
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
            matches!(err, ProbeSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_hash_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        let tampered = ProbeSlashData {
            hash: B256::ZERO,
            ..data
        };
        let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, ProbeSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_has_blob_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        let tampered = ProbeSlashData {
            has_blob: false,
            ..data
        };
        let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, ProbeSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_rate_per_mb_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        let tampered = ProbeSlashData {
            rate_per_mb: data.rate_per_mb + 1,
            ..data
        };
        let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, ProbeSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_timestamp_rejected() -> anyhow::Result<()> {
        let signer = PrivateKeySigner::random();
        let domain = sample_domain();
        let data = sample_data();

        let sig = data.sign(&signer, &domain)?;
        let tampered = ProbeSlashData {
            timestamp_us: data.timestamp_us + 1,
            ..data
        };
        let err = err_of(tampered.verify_signer(&sig, signer.address(), &domain))?;
        anyhow::ensure!(
            matches!(err, ProbeSlashError::WrongSigner { .. }),
            "expected WrongSigner, got: {err:?}"
        );
        Ok(())
    }

    /// Lock the EIP-712 type hash to the exact ADR 014 wording. If this
    /// breaks, either the ADR changed or the `sol!` macro's canonical
    /// encoding shifted — both warrant a coordinated update with the
    /// `SlashJudge` Solidity contract.
    #[test]
    fn probe_response_type_hash_matches_adr_014() -> anyhow::Result<()> {
        use alloy::primitives::keccak256;
        let canonical: &[u8] =
            b"ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)";
        let expected = keccak256(canonical);
        let actual = ProbeResponseSol::eip712_type_hash(&sample_data().to_sol());
        anyhow::ensure!(
            actual == expected,
            "ProbeResponse type hash drifted: actual={actual} expected={expected}"
        );
        Ok(())
    }

    /// Lock the domain separator to the canonical EIP-712 form.
    #[test]
    fn domain_separator_matches_eip712_canonical() -> anyhow::Result<()> {
        use alloy::primitives::{U256, keccak256};
        use alloy::sol_types::SolValue;

        let domain_typehash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let chain_id: u64 = 421_614;
        let verifying = address!("0000000000000000000000000000000000001234");

        let domain = slash_judge_domain(chain_id, verifying);
        let actual = domain.separator();

        let expected = keccak256(
            (
                domain_typehash,
                keccak256(SLASH_JUDGE_DOMAIN_NAME.as_bytes()),
                keccak256(SLASH_JUDGE_DOMAIN_VERSION.as_bytes()),
                U256::from(chain_id),
                verifying,
            )
                .abi_encode(),
        );
        anyhow::ensure!(
            actual == expected,
            "domain separator drift: actual={actual} expected={expected}"
        );
        Ok(())
    }

    /// Fixed-vector regression: a deterministic key + data must recover a
    /// specific address. Catches digest drift before the contract does.
    #[test]
    fn fixed_vector_recovers_expected_signer() -> anyhow::Result<()> {
        let pk_hex = "1111111111111111111111111111111111111111111111111111111111111111";
        let signer: PrivateKeySigner = pk_hex
            .parse()
            .map_err(|e| anyhow::anyhow!("parse test key: {e}"))?;
        let address = signer.address();

        let data = ProbeSlashData {
            hash: B256::repeat_byte(0xAA),
            has_blob: true,
            rate_per_mb: 1_000_000,
            timestamp_us: 1_700_000_000_000_000,
        };
        let domain = slash_judge_domain(
            421_614,
            address!("00000000000000000000000000000000deadbeef"),
        );

        // Pin the EIP-712 signing digest itself, not just self-consistent
        // sign→recover. Recovery alone passes even if the struct-hash/value
        // encoding drifts (it only proves the sig matches whatever digest
        // this code produced); a fixed digest locks on-chain `SlashJudge`
        // compatibility.
        let expected_hash = alloy::primitives::b256!(
            "67405dde5244ffc7846bda4886ef62ef29bcbb8ab34723314d63df181eabb3d0"
        );
        anyhow::ensure!(
            data.signing_hash(&domain) == expected_hash,
            "signing_hash drifted: actual={} expected={expected_hash}",
            data.signing_hash(&domain)
        );

        let sig = data.sign(&signer, &domain)?;
        let recovered = data.recover_signer(&sig, &domain)?;
        anyhow::ensure!(
            recovered == address,
            "recovered {recovered}, expected {address}"
        );
        Ok(())
    }
}
