//! Offline capability signing: sign an EIP-712 `Capability` for a delegate
//! signer against the sponsor's pool and serialize it as a `dcap1:` token.
//! Signing touches no chain.

use std::sync::Arc;

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256};
use alloy::signers::Signer;
use decdn_incentive::{Capability, CapabilityGrant, SignedCapability};
use sponsord_api::{IssuedCapability, MicroUsdc};

/// The largest terms the issuer grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest spending cap per capability, in micro-USDC.
    pub max_spending_cap: MicroUsdc,
    /// Longest time to expiry, in seconds from issue.
    pub max_ttl_secs: u64,
}

/// Requested terms; an omitted value means the maximum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TermsRequest {
    /// Spending cap in micro-USDC; `None` means [`Limits::max_spending_cap`].
    pub spending_cap: Option<MicroUsdc>,
    /// Seconds until expiry; `None` means [`Limits::max_ttl_secs`].
    pub ttl_secs: Option<u64>,
}

/// Resolved terms, within the issuer's [`Limits`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Terms {
    /// Spending cap, in micro-USDC.
    pub spending_cap: MicroUsdc,
    /// Seconds from issue until the capability expires.
    pub ttl_secs: u64,
}

/// Requested capability terms outside the issuer's bounds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TermsError {
    /// A requested value is above its maximum in [`Limits`].
    #[error("requested {field} {requested} exceeds the maximum {max}")]
    ExceedsMax {
        /// The offending term: `spending_cap` or `ttl_secs`.
        field: &'static str,
        /// The value asked for.
        requested: u64,
        /// The issuer's maximum for that term.
        max: u64,
    },
    /// A requested value is 0, which would register a dead signer.
    #[error("requested {field} is zero")]
    Zero {
        /// The offending term: `spending_cap` or `ttl_secs`.
        field: &'static str,
    },
}

/// Signs capped, expiring capabilities against one pool, as the pool owner,
/// within [`Limits`].
///
/// Any alloy [`Signer`] that signs raw hashes works: a local key, or a remote
/// one such as AWS or GCP KMS (not alloy's `LedgerSigner`, which refuses
/// `sign_hash`). With a local key, signing is deterministic (RFC 6979),
/// so equal terms give a byte-identical token; a remote signer may give a
/// different, equally valid signature each time.
pub struct Issuer {
    signer: Arc<dyn Signer + Send + Sync>,
    domain: Eip712Domain,
    pool_id: B256,
    limits: Limits,
}

// By hand: the signer may hold a key, so only its address is shown.
impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issuer")
            .field("signer", &self.signer.address())
            .field("pool_id", &self.pool_id)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Issuer {
    /// An issuer signing as `signer` for `pool_id`, under the `PaymentPool`'s
    /// EIP-712 `domain`. `signer` must be the pool's on-chain owner.
    #[must_use]
    pub fn new(
        signer: impl Signer + Send + Sync + 'static,
        domain: Eip712Domain,
        pool_id: B256,
        limits: Limits,
    ) -> Self {
        Self {
            signer: Arc::new(signer),
            domain,
            pool_id,
            limits,
        }
    }

    /// The pool-owner address these capabilities are signed by.
    #[must_use]
    pub fn owner_address(&self) -> Address {
        self.signer.address()
    }

    /// The pool these capabilities spend from.
    #[must_use]
    pub const fn pool_id(&self) -> B256 {
        self.pool_id
    }

    /// The largest terms this issuer grants.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    /// Resolve requested terms. An omitted value is the maximum.
    ///
    /// # Errors
    ///
    /// `TermsError::Zero` for a 0 (it would register a dead signer),
    /// `TermsError::ExceedsMax` for a value above the maximum.
    pub fn terms(&self, req: &TermsRequest) -> Result<Terms, TermsError> {
        Ok(Terms {
            spending_cap: MicroUsdc(bounded(
                "spending_cap",
                req.spending_cap.map(|c| c.0),
                self.limits.max_spending_cap.0,
            )?),
            ttl_secs: bounded("ttl_secs", req.ttl_secs, self.limits.max_ttl_secs)?,
        })
    }

    /// Sign a capability for `delegate` with exactly these terms.
    ///
    /// # Errors
    ///
    /// Propagates a signer error.
    pub async fn sign(
        &self,
        delegate: Address,
        spending_cap: MicroUsdc,
        expiry: u64,
    ) -> anyhow::Result<IssuedCapability> {
        let capability = Capability {
            signer: delegate,
            spending_cap: spending_cap.0,
            pool_id: self.pool_id,
            expiry,
        };
        let hash = capability.signing_hash(&self.domain);
        let signature = self
            .signer
            .sign_hash(&hash)
            .await
            .map_err(|e| anyhow::anyhow!("sign capability: {e}"))?;
        // Nodes and the contract accept only low-s signatures; a local key
        // already gives one, a remote signer may not.
        let signature = signature.normalize_s().unwrap_or(signature);
        let signed = SignedCapability {
            capability,
            signature,
        };
        Ok(IssuedCapability {
            token: CapabilityGrant::from_signed_capability(&signed).to_token(),
            spending_cap,
            expiry,
        })
    }
}

const fn bounded(field: &'static str, requested: Option<u64>, max: u64) -> Result<u64, TermsError> {
    match requested {
        None => Ok(max),
        Some(0) => Err(TermsError::Zero { field }),
        Some(v) if v > max => Err(TermsError::ExceedsMax {
            field,
            requested: v,
            max,
        }),
        Some(v) => Ok(v),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
