//! Offline capability signing: sign an EIP-712 `Capability` for a delegate
//! signer against the sponsor's pool and serialize it as a `dcap1:` token.
//! Signing touches no chain.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::{Capability, CapabilityGrant};

/// Requested capability terms outside the issuer's bounds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TermsError {
    #[error("requested {field} {requested} exceeds the maximum {max}")]
    ExceedsMax {
        field: &'static str,
        requested: u64,
        max: u64,
    },
    #[error("requested {field} is zero")]
    Zero { field: &'static str },
}

/// One signed capability: the `dcap1:` token and the terms it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedToken {
    pub token: String,
    pub spending_cap: u64,
    pub expiry: u64,
}

/// Signs capped, expiring capabilities against one pool, as the pool owner,
/// within a maximum cap and TTL.
pub struct Issuer {
    signer: PrivateKeySigner,
    domain: Eip712Domain,
    pool_id: B256,
    max_spending_cap: u64,
    max_ttl_secs: u64,
}

impl Issuer {
    #[must_use]
    pub fn new(
        signer: PrivateKeySigner,
        domain: Eip712Domain,
        pool_id: B256,
        max_spending_cap: u64,
        max_ttl_secs: u64,
    ) -> Self {
        Self {
            signer,
            domain,
            pool_id,
            max_spending_cap,
            max_ttl_secs,
        }
    }

    /// The pool-owner address these capabilities are signed by.
    #[must_use]
    pub fn owner_address(&self) -> Address {
        self.signer.address()
    }

    #[must_use]
    pub fn pool_id(&self) -> B256 {
        self.pool_id
    }

    #[must_use]
    pub fn max_spending_cap(&self) -> u64 {
        self.max_spending_cap
    }

    #[must_use]
    pub fn max_ttl_secs(&self) -> u64 {
        self.max_ttl_secs
    }

    /// Resolve requested terms into `(spending_cap, ttl_secs)`. An omitted
    /// value is the maximum.
    ///
    /// # Errors
    ///
    /// `TermsError::Zero` for a 0 (it would register a dead signer),
    /// `TermsError::ExceedsMax` for a value above the maximum.
    pub fn terms(
        &self,
        spending_cap: Option<u64>,
        ttl_secs: Option<u64>,
    ) -> Result<(u64, u64), TermsError> {
        Ok((
            bounded("spending_cap", spending_cap, self.max_spending_cap)?,
            bounded("ttl_secs", ttl_secs, self.max_ttl_secs)?,
        ))
    }

    /// Sign a capability for `delegate` with exactly these terms. Signing is
    /// deterministic (RFC 6979), so equal terms give a byte-identical token.
    ///
    /// # Errors
    ///
    /// Propagates a signer error.
    pub fn sign(
        &self,
        delegate: Address,
        spending_cap: u64,
        expiry: u64,
    ) -> anyhow::Result<SignedToken> {
        let capability = Capability {
            signer: delegate,
            spending_cap,
            pool_id: self.pool_id,
            expiry,
        };
        let signed = capability
            .sign(&self.signer, &self.domain)
            .map_err(|e| anyhow::anyhow!("sign capability: {e}"))?;
        Ok(SignedToken {
            token: CapabilityGrant::from_signed_capability(&signed).to_token(),
            spending_cap,
            expiry,
        })
    }
}

fn bounded(field: &'static str, requested: Option<u64>, max: u64) -> Result<u64, TermsError> {
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
    use super::*;
    use decdn_incentive::{CapabilityGrant, voucher_domain};

    fn issuer(max_cap: u64, max_ttl: u64) -> (Issuer, Address, Eip712Domain) {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let domain = voucher_domain(421_614, Address::repeat_byte(0x22));
        let issuer = Issuer::new(
            signer,
            domain.clone(),
            B256::repeat_byte(0x11),
            max_cap,
            max_ttl,
        );
        (issuer, owner, domain)
    }

    #[test]
    fn signed_token_decodes_and_recovers_owner_and_fields() {
        let (issuer, owner, domain) = issuer(10_000_000, 2_592_000);
        let delegate = Address::repeat_byte(0xaa);
        let signed = issuer.sign(delegate, 7_000_000, 1_769_904_000).unwrap();

        assert!(signed.token.starts_with("dcap1:"));
        assert_eq!(signed.spending_cap, 7_000_000);
        assert_eq!(signed.expiry, 1_769_904_000);

        let grant = CapabilityGrant::from_token(&signed.token).unwrap();
        assert_eq!(grant.signer, delegate);
        assert_eq!(grant.pool_id, B256::repeat_byte(0x11));
        assert_eq!(grant.spending_cap, 7_000_000);
        assert_eq!(grant.expiry, 1_769_904_000);
        assert_eq!(grant.owner(&domain).unwrap(), owner);
    }

    #[test]
    fn signing_is_deterministic() {
        let (issuer, _, _) = issuer(10_000_000, 2_592_000);
        let delegate = Address::repeat_byte(0xaa);
        let a = issuer.sign(delegate, 5_000_000, 1_769_904_000).unwrap();
        let b = issuer.sign(delegate, 5_000_000, 1_769_904_000).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn terms_default_to_max_and_honour_lower_values() {
        let (issuer, _, _) = issuer(5_000_000, 172_800);
        assert_eq!(issuer.terms(None, None), Ok((5_000_000, 172_800)));
        assert_eq!(issuer.terms(Some(1), Some(60)), Ok((1, 60)));
        assert_eq!(
            issuer.terms(Some(5_000_000), Some(172_800)),
            Ok((5_000_000, 172_800))
        );
    }

    #[test]
    fn terms_reject_zero_and_above_max() {
        let (issuer, _, _) = issuer(5_000_000, 172_800);
        assert_eq!(
            issuer.terms(Some(5_000_001), None),
            Err(TermsError::ExceedsMax {
                field: "spending_cap",
                requested: 5_000_001,
                max: 5_000_000
            })
        );
        assert_eq!(
            issuer.terms(None, Some(172_801)),
            Err(TermsError::ExceedsMax {
                field: "ttl_secs",
                requested: 172_801,
                max: 172_800
            })
        );
        assert_eq!(
            issuer.terms(Some(0), None),
            Err(TermsError::Zero {
                field: "spending_cap"
            })
        );
        assert_eq!(
            issuer.terms(None, Some(0)),
            Err(TermsError::Zero { field: "ttl_secs" })
        );
    }
}
