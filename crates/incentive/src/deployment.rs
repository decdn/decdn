//! The `PaymentPool` deployment a piece of pool state belongs to.
//!
//! `poolId = keccak256(owner, ownerPoolNonce)` names neither the chain nor the
//! contract, and a redeploy restarts every owner's nonce, so a pool id repeats
//! across deployments. Every persisted key that names a pool id therefore
//! starts with the [`Deployment`] it lives on, so rows of several
//! `PaymentPool` contracts share one store without colliding.
//!
//! On the wire a deployment is a CAIP-10 [`PoolAccount`] in the `eip155`
//! namespace ([`Deployment::pool_account`], [`Deployment::from_pool_account`]).

use alloy::primitives::Address;
use alloy::sol_types::Eip712Domain;
use decdn_protocol::PoolAccount;

use crate::store::StoreError;
use crate::voucher::voucher_domain;

/// The `PaymentPool` deployment a row belongs to: the payment chain and the
/// contract address that make up the voucher and capability EIP-712 domain.
///
/// A row is only meaningful against the deployment that wrote it. Two
/// deployments that share a contract address on different chains, or a chain
/// with different contract addresses, are different deployments.
///
/// Ordered by chain id, then address, so a store key that starts with
/// [`Self::to_bytes`] sorts the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

    /// The CAIP-10 account that names this pool on the wire:
    /// `eip155:<chain_id>:0x<address>`.
    #[must_use]
    pub fn pool_account(self) -> PoolAccount {
        PoolAccount::eip155(self.chain_id, self.payment_pool.into_array())
    }

    /// The deployment a wire [`PoolAccount`] names, or `None` when the account
    /// is not an `eip155` contract address (another namespace, a chain
    /// reference that is not a `u64`, or an address that is not 20 hex bytes).
    #[must_use]
    pub fn from_pool_account(account: &PoolAccount) -> Option<Self> {
        Some(Self {
            chain_id: account.chain_id().eip155_id()?,
            payment_pool: Address::from(account.eip155_address()?),
        })
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
mod tests;
