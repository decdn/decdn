//! The `PaymentPool` deployment a piece of pool state belongs to.
//!
//! `poolId = keccak256(owner, ownerPoolNonce)` names neither the chain nor the
//! contract, and a redeploy restarts every owner's nonce, so a pool id repeats
//! across deployments. Every persisted row that names a pool id therefore also
//! carries the [`Deployment`] that wrote it: the seller-side lane store keeps
//! one stamp for all its rows, and each buyer pool row carries its own tag
//! ([`crate::buyer_pool::BuyerPoolState::deployment`]).

use alloy::primitives::Address;
use alloy::sol_types::Eip712Domain;

use crate::store::StoreError;
use crate::voucher::voucher_domain;

/// The `PaymentPool` deployment a row belongs to: the chain and contract
/// address that make up the voucher EIP-712 domain.
///
/// A row is only meaningful against the deployment that wrote it. Two
/// deployments that share a contract address on different chains, or a chain
/// with different contract addresses, are different deployments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Deployment {
    /// EIP-712 `chainId` of the voucher domain.
    pub chain_id: u64,
    /// The `PaymentPool` contract, the voucher domain's `verifyingContract`.
    /// The nonzero check lives at the config boundary
    /// (`decdn_common::address::parse_nonzero_address`); a new construction
    /// site validates there, not here.
    pub payment_pool: Address,
}

impl Deployment {
    /// Byte width of an encoded [`Deployment`]: `8 + 20`.
    pub const ENCODED_LEN: usize = 28;

    /// The voucher EIP-712 domain of this deployment.
    #[must_use]
    pub fn voucher_domain(self) -> Eip712Domain {
        voucher_domain(self.chain_id, self.payment_pool)
    }

    /// Encode as `chain_id (8 bytes, big-endian) ‖ payment_pool (20 bytes)`.
    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&self.chain_id.to_be_bytes());
        out[8..].copy_from_slice(self.payment_pool.as_slice());
        out
    }

    /// Decode the [`Self::to_bytes`] form.
    ///
    /// # Errors
    ///
    /// [`StoreError::Corrupt`] for any width other than [`Self::ENCODED_LEN`]:
    /// a deployment decides which rows a reader trusts, so bytes this binary
    /// cannot read must not decode to some deployment.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        let corrupt = || StoreError::Corrupt {
            pool_id: None,
            detail: format!(
                "deployment is {} bytes, expected {}",
                bytes.len(),
                Self::ENCODED_LEN
            ),
        };
        let bytes: &[u8; Self::ENCODED_LEN] = bytes.try_into().map_err(|_| corrupt())?;
        // Width is proven above; these splits re-prove it to the compiler
        // without reaching for a panicking accessor.
        let (chain, pool) = bytes.split_first_chunk::<8>().ok_or_else(corrupt)?;
        let pool: &[u8; 20] = pool.try_into().map_err(|_| corrupt())?;
        Ok(Self {
            chain_id: u64::from_be_bytes(*chain),
            payment_pool: Address::from(*pool),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEPLOYMENT: Deployment = Deployment {
        chain_id: 0x0102_0304_0506_0708,
        payment_pool: Address::repeat_byte(0x9c),
    };

    #[test]
    fn bytes_round_trip() -> anyhow::Result<()> {
        let bytes = DEPLOYMENT.to_bytes();
        anyhow::ensure!(
            bytes[..8] == [1, 2, 3, 4, 5, 6, 7, 8],
            "chain id is big-endian first"
        );
        anyhow::ensure!(bytes[8..] == [0x9c; 20], "the address follows the chain id");
        anyhow::ensure!(Deployment::from_bytes(&bytes)? == DEPLOYMENT);
        Ok(())
    }

    #[test]
    fn a_wrong_width_is_corrupt() {
        for len in [0, 20, 27, 29] {
            assert!(
                matches!(
                    Deployment::from_bytes(&vec![0u8; len]),
                    Err(StoreError::Corrupt { .. })
                ),
                "{len} bytes must not decode"
            );
        }
    }

    #[test]
    fn the_voucher_domain_names_the_chain_and_the_contract() {
        let domain = DEPLOYMENT.voucher_domain();
        assert_eq!(
            domain,
            voucher_domain(DEPLOYMENT.chain_id, DEPLOYMENT.payment_pool)
        );
    }
}
