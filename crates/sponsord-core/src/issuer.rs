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
mod tests {
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::voucher_domain;

    use super::*;

    fn issuer(max_cap: u64, max_ttl: u64) -> (Issuer, Address, Eip712Domain) {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let domain = voucher_domain(421_614, Address::repeat_byte(0x22));
        let issuer = Issuer::new(
            signer,
            domain.clone(),
            B256::repeat_byte(0x11),
            Limits {
                max_spending_cap: MicroUsdc(max_cap),
                max_ttl_secs: max_ttl,
            },
        );
        (issuer, owner, domain)
    }

    fn req(cap: Option<u64>, ttl: Option<u64>) -> TermsRequest {
        TermsRequest {
            spending_cap: cap.map(MicroUsdc),
            ttl_secs: ttl,
        }
    }

    #[tokio::test]
    async fn signed_token_decodes_and_recovers_owner_and_fields() {
        let (issuer, owner, domain) = issuer(10_000_000, 2_592_000);
        let delegate = Address::repeat_byte(0xaa);
        let signed = issuer
            .sign(delegate, MicroUsdc(7_000_000), 1_769_904_000)
            .await
            .unwrap();

        assert!(signed.token.starts_with("dcap1:"));
        assert_eq!(signed.spending_cap, MicroUsdc(7_000_000));
        assert_eq!(signed.expiry, 1_769_904_000);

        let grant = CapabilityGrant::from_token(&signed.token).unwrap();
        assert_eq!(grant.signer, delegate);
        assert_eq!(grant.pool_id, B256::repeat_byte(0x11));
        assert_eq!(grant.spending_cap, 7_000_000);
        assert_eq!(grant.expiry, 1_769_904_000);
        assert_eq!(grant.owner(&domain).unwrap(), owner);
    }

    #[tokio::test]
    async fn local_signing_is_deterministic() {
        let (issuer, _, _) = issuer(10_000_000, 2_592_000);
        let delegate = Address::repeat_byte(0xaa);
        let a = issuer
            .sign(delegate, MicroUsdc(5_000_000), 1_769_904_000)
            .await
            .unwrap();
        let b = issuer
            .sign(delegate, MicroUsdc(5_000_000), 1_769_904_000)
            .await
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn terms_default_to_max_and_honour_lower_values() {
        let (issuer, _, _) = issuer(5_000_000, 172_800);
        let terms = |cap, ttl| issuer.terms(&req(cap, ttl));
        let t = |cap, ttl_secs| Terms {
            spending_cap: MicroUsdc(cap),
            ttl_secs,
        };
        assert_eq!(terms(None, None), Ok(t(5_000_000, 172_800)));
        assert_eq!(terms(Some(1), Some(60)), Ok(t(1, 60)));
        assert_eq!(
            terms(Some(5_000_000), Some(172_800)),
            Ok(t(5_000_000, 172_800))
        );
    }

    #[test]
    fn terms_reject_zero_and_above_max() {
        let (issuer, _, _) = issuer(5_000_000, 172_800);
        let terms = |cap, ttl| issuer.terms(&req(cap, ttl));
        assert_eq!(
            terms(Some(5_000_001), None),
            Err(TermsError::ExceedsMax {
                field: "spending_cap",
                requested: 5_000_001,
                max: 5_000_000
            })
        );
        assert_eq!(
            terms(None, Some(172_801)),
            Err(TermsError::ExceedsMax {
                field: "ttl_secs",
                requested: 172_801,
                max: 172_800
            })
        );
        assert_eq!(
            terms(Some(0), None),
            Err(TermsError::Zero {
                field: "spending_cap"
            })
        );
        assert_eq!(
            terms(None, Some(0)),
            Err(TermsError::Zero { field: "ttl_secs" })
        );
    }
}
