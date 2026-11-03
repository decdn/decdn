//! Origin attestation checks (ADR 022 § Origin Attestation).
//!
//! An attestation is a seated origin's signed `StreamResponse` with `ok: true`
//! for a hash. It is valid when its signature recovers to an operator that
//! holds an origin seat now and that this node's deny-set does not name.
//! Validity reads nothing else: not the signed timestamp, rate or size. So an
//! attestation has no expiry, and unseating its signer cancels it at the next
//! check.
//!
//! [`AttestationVerifier`] is the one check every caller shares: the DHT
//! receiver before it stores a record, the lookup before it probes a provider,
//! and the pull leg before it keeps an upstream's attestation.

use std::sync::Arc;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::Address;
use decdn_incentive::StreamSlashData;
use decdn_protocol::OriginAttestation;

use crate::content_deny::ContentDenylist;
use crate::dht::seated_origins::SeatedOrigins;

/// Verifies origin attestations against the seated-origin set.
#[derive(Clone)]
pub struct AttestationVerifier {
    seated: Arc<dyn SeatedOrigins>,
    deny: Arc<ContentDenylist>,
    domain: Eip712Domain,
}

impl std::fmt::Debug for AttestationVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttestationVerifier")
            .field("seated", &self.seated.len())
            .finish_non_exhaustive()
    }
}

impl AttestationVerifier {
    /// Build a verifier over the seated-origin set, the node's origin
    /// deny-set and the `SlashJudge` EIP-712 domain the signatures use.
    #[must_use]
    pub fn new(
        seated: Arc<dyn SeatedOrigins>,
        deny: Arc<ContentDenylist>,
        domain: Eip712Domain,
    ) -> Self {
        Self {
            seated,
            deny,
            domain,
        }
    }

    /// The EIP-712 domain attestations are signed under.
    #[must_use]
    pub const fn domain(&self) -> &Eip712Domain {
        &self.domain
    }

    /// The operator whose signature `attestation` carries for `hash`, or
    /// `None` when the signature does not recover. Says nothing about
    /// validity; see [`Self::admits`].
    #[must_use]
    pub fn recover(&self, hash: [u8; 32], attestation: &OriginAttestation) -> Option<Address> {
        StreamSlashData::recover_attestation_signer(hash, attestation, &self.domain).ok()
    }

    /// Whether `signer` may attest now: it holds an origin seat and the
    /// deny-set does not name it.
    #[must_use]
    pub fn admits(&self, signer: &Address) -> bool {
        self.seated.is_seated(signer) && !self.deny.is_origin_denied(signer)
    }

    /// The signer of a valid `attestation` for `hash`, or `None` when it is
    /// not valid.
    #[must_use]
    pub fn verify(&self, hash: [u8; 32], attestation: &OriginAttestation) -> Option<Address> {
        self.recover(hash, attestation)
            .filter(|signer| self.admits(signer))
    }
}

/// Sign an origin attestation for `hash` as `signer`. An origin signs its own
/// this way; tests use it to build valid records.
///
/// # Errors
///
/// Propagates a signer failure.
pub fn sign_attestation<S: alloy::signers::SignerSync>(
    signer: &S,
    domain: &Eip712Domain,
    hash: [u8; 32],
    rate_per_mb: u64,
    total_bytes: u64,
    timestamp_us: u64,
) -> anyhow::Result<OriginAttestation> {
    let mut attestation = OriginAttestation {
        rate_per_mb,
        total_bytes,
        timestamp_us,
        slash_sig: Vec::new(),
    };
    let sig = StreamSlashData::from_response_body(&attestation.signed_body(hash))
        .sign(signer, domain)
        .map_err(|e| anyhow::anyhow!("sign origin attestation: {e}"))?;
    attestation.slash_sig = sig.as_bytes().to_vec();
    Ok(attestation)
}

/// Fixtures for tests that need valid attestations: one seated origin key, a
/// verifier that admits it, and a signer for any hash.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
