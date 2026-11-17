//! Capability scopes: the record that limits a capability registered on a
//! gateway to the namespaces its pool owner names.
//!
//! A scope is an owner-signed EIP-712 record over the `PaymentPool`
//! deployment the capability spends on:
//!
//! ```text
//! CapabilityScope(address signer,bytes32 poolId,uint256[] namespaces)
//! ```
//!
//! The domain is `decdn-capability-scope` version `1` with the chain id and
//! the `PaymentPool` as `verifyingContract`, so a scope signed for one
//! deployment means nothing on another. The record names the delegate key and
//! the pool of one capability, so it binds to that capability alone. The
//! gateway takes a scope whose signature recovers the pool's on-chain owner,
//! the same owner the capability's own signature must recover. On a public
//! gateway, a scoped capability pays only for the requests a namespace
//! pointer serves: a request on `<ns>.<host>` for the pointer's own bundle.
//! The pointer is signed by the namespace's owner, so a stranger can make the
//! capability pay only for content its owner chose to serve. On a loopback
//! gateway, only the local user's own clients reach it, so the namespace a
//! request's `decdn:` URI names selects it. `decdn pool assign --namespace`
//! signs one with the pool owner's keystore.
//!
//! A registration names its [`Scope`] explicitly: a signed scope, or `"any"`
//! for a capability that pays for any request.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, Bytes, Signature, U256};
use alloy::signers::SignerSync;
use alloy::sol_types::{SolStruct as _, eip712_domain};

use super::pointer::MalformedSignature;

// The Rust struct name is the EIP-712 type name, so it is `CapabilityScope`
// verbatim.
mod sol_types {
    alloy::sol! {
        #[allow(non_snake_case, missing_debug_implementations)]
        struct CapabilityScope {
            address signer;
            bytes32 poolId;
            uint256[] namespaces;
        }
    }
}

/// The EIP-712 domain a scope is signed under, for the `PaymentPool` at
/// `payment_pool` on `chain_id`.
pub(crate) const fn scope_domain(chain_id: u64, payment_pool: Address) -> Eip712Domain {
    eip712_domain! {
        name: "decdn-capability-scope",
        version: "1",
        chain_id: chain_id,
        verifying_contract: payment_pool,
    }
}

/// A scope as `PUT /_capability` carries it beside the capability:
/// `{"namespaces", "signature"}`, the ids as numbers and the 65-byte
/// `r‖s‖v` signature as `0x` hex. The delegate key and the pool come from the
/// capability it is registered with.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignedScope {
    /// The namespaces whose pointers the capability may pay for.
    pub(crate) namespaces: Vec<u64>,
    /// The pool owner's EIP-712 signature over the delegate key, the pool and
    /// the namespaces.
    pub(crate) signature: Bytes,
}

/// What a registered capability pays for, as `PUT /_capability` states it:
/// `"any"` for any request, or a signed namespace scope. A registration
/// always names one, so a capability pays for any request only when its
/// registrant says so.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "RawScope", into = "RawScope")]
pub(crate) enum Scope {
    /// The capability pays for any request.
    Any,
    /// The capability pays only for its namespaces' requests.
    Namespaces(SignedScope),
}

/// The JSON shape of a [`Scope`]: the word `"any"`, or a [`SignedScope`].
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum RawScope {
    Word(String),
    Signed(SignedScope),
}

/// The word a [`Scope::Any`] is written as.
const ANY: &str = "any";

impl TryFrom<RawScope> for Scope {
    type Error = String;

    fn try_from(raw: RawScope) -> Result<Self, String> {
        match raw {
            RawScope::Word(word) if word == ANY => Ok(Self::Any),
            RawScope::Word(word) => Err(format!(
                "a scope is \"{ANY}\" or {{\"namespaces\", \"signature\"}}, not \"{word}\""
            )),
            RawScope::Signed(scope) => Ok(Self::Namespaces(scope)),
        }
    }
}

impl From<Scope> for RawScope {
    fn from(scope: Scope) -> Self {
        match scope {
            Scope::Any => Self::Word(ANY.to_owned()),
            Scope::Namespaces(scope) => Self::Signed(scope),
        }
    }
}

fn signing_hash(signer: Address, pool: B256, namespaces: &[u64], domain: &Eip712Domain) -> B256 {
    sol_types::CapabilityScope {
        signer,
        poolId: pool,
        namespaces: namespaces.iter().map(|ns| U256::from(*ns)).collect(),
    }
    .eip712_signing_hash(domain)
}

impl SignedScope {
    /// Sign a scope that limits the capability of delegate `signer` on
    /// `pool` to `namespaces`, with the pool owner's `owner` under `domain`.
    ///
    /// # Errors
    ///
    /// The signer's own error.
    pub(crate) fn sign<S: SignerSync>(
        owner: &S,
        signer: Address,
        pool: B256,
        namespaces: Vec<u64>,
        domain: &Eip712Domain,
    ) -> Result<Self, alloy::signers::Error> {
        let hash = signing_hash(signer, pool, &namespaces, domain);
        let signature = owner.sign_hash_sync(&hash)?;
        Ok(Self {
            namespaces,
            signature: Bytes::from(signature.as_bytes().to_vec()),
        })
    }

    /// The address that signed this scope for delegate `signer` on `pool`
    /// under `domain`.
    ///
    /// # Errors
    ///
    /// [`MalformedSignature`]: the bytes are no signature, or a high-`s` one.
    pub(crate) fn owner(
        &self,
        signer: Address,
        pool: B256,
        domain: &Eip712Domain,
    ) -> Result<Address, MalformedSignature> {
        let signature =
            Signature::try_from(self.signature.as_ref()).map_err(|_| MalformedSignature)?;
        if decdn_incentive::sig_canon::is_high_s(&signature) {
            return Err(MalformedSignature);
        }
        let hash = signing_hash(signer, pool, &self.namespaces, domain);
        signature
            .recover_address_from_prehash(&hash)
            .map_err(|_| MalformedSignature)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
