//! Offline capability issuance: sign an EIP-712 `Capability` for a delegate
//! signer against the sponsor's pool and serialize it as a `dcap1:` token.
//! Signing touches no chain — this is the zero-tx allowance.

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_incentive::{Capability, CapabilityGrant};

/// Signs capped, expiring capabilities against one pool, as the pool owner.
pub struct Issuer {
    signer: PrivateKeySigner,
    domain: Eip712Domain,
    pool_id: B256,
    spending_cap: u64,
    ttl_secs: u64,
}

impl Issuer {
    #[must_use]
    pub fn new(
        signer: PrivateKeySigner,
        domain: Eip712Domain,
        pool_id: B256,
        spending_cap: u64,
        ttl_secs: u64,
    ) -> Self {
        Self {
            signer,
            domain,
            pool_id,
            spending_cap,
            ttl_secs,
        }
    }

    /// The pool-owner address these capabilities are signed by.
    #[must_use]
    pub fn owner_address(&self) -> Address {
        self.signer.address()
    }

    /// Sign a capability delegating capped spend to `delegate`, expiring
    /// `ttl_secs` after `now_unix`. Returns the `dcap1:` token and the expiry.
    ///
    /// # Errors
    ///
    /// Propagates a signer error (locked/remote key).
    pub fn issue(&self, delegate: Address, now_unix: u64) -> anyhow::Result<(String, u64)> {
        let expiry = now_unix.saturating_add(self.ttl_secs);
        let capability = Capability {
            signer: delegate,
            spending_cap: U256::from(self.spending_cap),
            pool_id: self.pool_id,
            expiry,
        };
        let signed = capability
            .sign(&self.signer, &self.domain)
            .map_err(|e| anyhow::anyhow!("sign capability: {e}"))?;
        let token = CapabilityGrant::from_signed_capability(&signed).to_token();
        Ok((token, expiry))
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

    #[test]
    fn issued_token_decodes_and_recovers_owner_and_fields() {
        let signer = PrivateKeySigner::random();
        let owner = signer.address();
        let pool_id = B256::repeat_byte(0x11);
        let domain = voucher_domain(421_614, Address::repeat_byte(0x22));
        let issuer = Issuer::new(signer, domain.clone(), pool_id, 10_000_000, 2_592_000);

        let delegate = Address::repeat_byte(0xaa);
        let now = 1_769_904_000u64;
        let (token, expiry) = issuer.issue(delegate, now).unwrap();

        assert!(token.starts_with("dcap1:"));
        assert_eq!(expiry, now + 2_592_000);

        let grant = CapabilityGrant::from_token(&token).unwrap();
        assert_eq!(grant.signer, delegate);
        assert_eq!(grant.pool_id, pool_id);
        assert_eq!(grant.spending_cap, U256::from(10_000_000u64));
        assert_eq!(grant.expiry, expiry);
        assert_eq!(grant.owner(&domain).unwrap(), owner);
    }
}
