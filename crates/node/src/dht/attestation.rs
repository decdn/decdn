//! Origin attestation checks (ADR 022 § Origin Attestation).
//!
//! An attestation is a seated origin's signed statement that it serves a hash
//! in a namespace for a bounded time. It is valid when all of these hold:
//!
//! - its namespace is not 0, and its signature recovers to an operator that
//!   holds an origin seat in that namespace now;
//! - the operator's `NodeId` is in the node's current registered set: the
//!   `CapacityBond` registry snapshot binds the operator to a `NodeId`, and
//!   the staker set reports that `NodeId` active. A seat outlives its
//!   operator's registration until someone prunes it, so the seat alone does
//!   not admit;
//! - this node's deny-set does not name the operator;
//! - its lifetime lies in `[MIN_ATTESTATION_TTL_SEC, MAX_ATTESTATION_TTL_SEC]`;
//! - it is live: `now − ttl < timestamp ≤ now + MAX_ATTESTATION_FUTURE_SKEW_US`.
//!
//! So an attestation expires on its own, and unseating its signer from its
//! namespace, or its leaving the registered set, cancels it at the next check.
//!
//! [`AttestationVerifier`] is the one check every caller shares: the DHT
//! receiver before it stores a record and before it answers with one, the
//! lookup before it opens a provider, the pull leg before it keeps an
//! upstream's attestation, and the serve gate before it serves a hash.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, U256};
use decdn_protocol::OriginAttestation;

use crate::content_deny::ContentDenylist;
use crate::dht::routing::NodeId;
use crate::dht::seated_origins::SeatedOrigins;
use crate::dht::staker_set::StakerSet;

/// The node's current registered set, read per operator: the registry
/// snapshot's `operator → NodeId` binding and the staker set's liveness. Both
/// halves are live, so a registry refresh takes effect at the next check.
#[derive(Clone)]
pub struct RegisteredOperators {
    operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    staker_set: Arc<dyn StakerSet>,
}

impl std::fmt::Debug for RegisteredOperators {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredOperators")
            .field("active", &self.staker_set.len())
            .finish_non_exhaustive()
    }
}

impl RegisteredOperators {
    /// The registered set over the registry snapshot's reverse projection and
    /// the shared staker set.
    #[must_use]
    pub fn new(
        operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
        staker_set: Arc<dyn StakerSet>,
    ) -> Self {
        Self {
            operator_to_node,
            staker_set,
        }
    }

    /// Whether `operator` is bound to a `NodeId` the staker set reports
    /// active. An operator with no binding is not registered.
    #[must_use]
    pub fn is_registered(&self, operator: &Address) -> bool {
        crate::dht::chain_projection::with_read(
            &self.operator_to_node,
            "chain operator reverse map",
            |rev| rev.get(operator).copied(),
        )
        .is_some_and(|node_id| self.staker_set.is_active(&node_id))
    }
}

/// Verifies origin attestations against the seated-origin set and the
/// registered set.
#[derive(Clone)]
pub struct AttestationVerifier {
    seated: Arc<dyn SeatedOrigins>,
    registered: RegisteredOperators,
    deny: Arc<ContentDenylist>,
    domain: Eip712Domain,
}

impl std::fmt::Debug for AttestationVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttestationVerifier")
            .field("seated", &self.seated.len())
            .field("registered", &self.registered)
            .finish_non_exhaustive()
    }
}

impl AttestationVerifier {
    /// Build a verifier over the seated-origin set, the registered set, the
    /// node's origin deny-set and the `CapacityBond` EIP-712 domain the
    /// signatures use.
    #[must_use]
    pub fn new(
        seated: Arc<dyn SeatedOrigins>,
        registered: RegisteredOperators,
        deny: Arc<ContentDenylist>,
        domain: Eip712Domain,
    ) -> Self {
        Self {
            seated,
            registered,
            deny,
            domain,
        }
    }

    /// The EIP-712 domain attestations are signed under.
    #[must_use]
    pub const fn domain(&self) -> &Eip712Domain {
        &self.domain
    }

    /// The lowest namespace `operator` holds a seat in, when it may attest
    /// there: the namespace a seated node signs its own attestations in.
    #[must_use]
    pub fn attesting_namespace(&self, operator: &Address) -> Option<U256> {
        self.seated
            .first_namespace(operator)
            .filter(|namespace| self.admits(*namespace, operator))
    }

    /// Ask the seat set for an early re-read, rate-limited by the seat
    /// snapshot. A node whose own backend holds a hash it cannot attest calls
    /// this, so a newly seated origin attests its content within a minute.
    pub fn request_seat_refresh(&self) {
        self.seated.request_refresh();
    }

    /// The operator whose signature `attestation` carries for `hash`, or
    /// `None` when the signature does not recover. Says nothing about
    /// validity; see [`Self::verify`].
    #[must_use]
    pub fn recover(&self, hash: [u8; 32], attestation: &OriginAttestation) -> Option<Address> {
        decdn_incentive::recover_origin_attestation_signer(hash, attestation, &self.domain).ok()
    }

    /// Whether `signer` may attest in `namespace` now: the namespace is not
    /// 0, `signer` holds a seat in it, `signer` is in the registered set, and
    /// the deny-set does not name it.
    #[must_use]
    pub fn admits(&self, namespace: U256, signer: &Address) -> bool {
        !namespace.is_zero()
            && self.seated.is_seated_in(namespace, signer)
            && self.registered.is_registered(signer)
            && !self.deny.is_origin_denied(signer)
    }

    /// The signer of `attestation` for `hash` when it is valid at `now_us`,
    /// else `None`.
    #[must_use]
    pub fn verify_at(
        &self,
        hash: [u8; 32],
        attestation: &OriginAttestation,
        now_us: u64,
    ) -> Option<Address> {
        if !attestation.ttl_in_bounds() || !attestation.is_live_at(now_us) {
            return None;
        }
        self.recover(hash, attestation)
            .filter(|signer| self.still_valid(attestation, signer, now_us))
    }

    /// Whether `attestation`, whose signature already recovered to `signer`,
    /// is valid at `now_us`. A caller that cached the signer skips the
    /// recovery with this.
    #[must_use]
    pub fn still_valid(
        &self,
        attestation: &OriginAttestation,
        signer: &Address,
        now_us: u64,
    ) -> bool {
        attestation.ttl_in_bounds()
            && attestation.is_live_at(now_us)
            && self.admits(U256::from_be_bytes(attestation.namespace_id), signer)
    }

    /// The signer of `attestation` for `hash` when it is valid now, else
    /// `None`.
    #[must_use]
    pub fn verify(&self, hash: [u8; 32], attestation: &OriginAttestation) -> Option<Address> {
        self.verify_at(hash, attestation, crate::handlers::timestamp_skew::now_us())
    }
}

/// Sign an origin attestation for `hash` in `namespace` as `signer`, stamped
/// `timestamp_us` with a lifetime of `ttl_sec`. A seated origin signs its own
/// this way; tests use it to build valid records.
///
/// # Errors
///
/// Propagates a signer failure.
pub fn sign_attestation<S: alloy::signers::SignerSync>(
    signer: &S,
    domain: &Eip712Domain,
    hash: [u8; 32],
    namespace: U256,
    timestamp_us: u64,
    ttl_sec: u32,
) -> anyhow::Result<OriginAttestation> {
    decdn_incentive::sign_origin_attestation(
        signer,
        domain,
        hash,
        namespace.to_be_bytes(),
        timestamp_us,
        ttl_sec,
    )
    .map_err(|e| anyhow::anyhow!("sign origin attestation: {e}"))
}

/// Fixtures for tests that need valid attestations: one seated origin key, a
/// verifier that admits it, and a signer for any hash.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
