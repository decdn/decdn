//! Which configured `PaymentPool` contract a capability spends from (ADR 012
//! § Delegated capabilities). A capability's EIP-712 domain binds one
//! contract, its payment chain id and address, so its owner signature
//! recovers to the pool's owner under that contract's domain only. The
//! gateway's capability registration and `decdn get --capability` both
//! resolve the pool here.

use alloy::primitives::Address;
use decdn_client::session::PaymentPoolChain;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{CapabilityGrant, Deployment, PoolId};

/// Why a capability resolves to no configured pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CapabilityPoolError {
    /// The capability's owner signature is malformed.
    Malformed(String),
    /// Every configured pool was read, and none has an owner that signed the
    /// capability.
    NoMatch(String),
    /// No configured pool matches, and the owner read of one failed.
    Unread(String),
}

impl std::fmt::Display for CapabilityPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(text) | Self::NoMatch(text) | Self::Unread(text) => {
                f.write_str(text.trim_end())
            }
        }
    }
}

impl std::error::Error for CapabilityPoolError {}

/// Whether a pool whose on-chain owner is `on_chain` accepts a capability its
/// signature recovers to `signed_by`.
pub(crate) fn owner_matches(
    on_chain: Address,
    signed_by: Address,
    pool_id: PoolId,
) -> Result<(), String> {
    if on_chain.is_zero() {
        Err(format!(
            "pool {pool_id} does not exist on this deployment\n"
        ))
    } else if on_chain != signed_by {
        Err(format!(
            "the capability is not signed by the owner of pool {pool_id}\n"
        ))
    } else {
        Ok(())
    }
}

/// The configured `PaymentPool` contract `grant` spends from. Each configured
/// pool is tried in order: the signer the grant recovers to under its
/// domain, against `read_owner`'s on-chain owner of `grant.pool_id` there.
/// The first pool where they match is the capability's.
///
/// # Errors
///
/// [`CapabilityPoolError::Malformed`] when the signature is malformed,
/// [`CapabilityPoolError::Unread`] when no pool matches and a read failed,
/// and [`CapabilityPoolError::NoMatch`] when every pool was read and none
/// matches.
pub(crate) async fn capability_pool<F, Fut>(
    grant: &CapabilityGrant,
    pools: &[PaymentPoolChain],
    read_owner: F,
) -> Result<Deployment, CapabilityPoolError>
where
    F: Fn(&PaymentPoolChain, PoolId) -> Fut,
    Fut: std::future::Future<Output = Result<Address, String>>,
{
    let mut owners = Vec::with_capacity(pools.len());
    let mut unread = None;
    for pool in pools {
        let signed_by = grant
            .owner(&pool.deployment.voucher_domain())
            .map_err(|e| CapabilityPoolError::Malformed(format!("{e}\n")))?;
        match read_owner(pool, grant.pool_id).await {
            Ok(on_chain) if owner_matches(on_chain, signed_by, grant.pool_id).is_ok() => {
                return Ok(pool.deployment);
            }
            Ok(on_chain) => owners.push((on_chain, signed_by)),
            Err(e) => {
                unread.get_or_insert(e);
            }
        }
    }
    if let Some(e) = unread {
        return Err(CapabilityPoolError::Unread(format!(
            "could not read the capability's pool on-chain: {e}\n"
        )));
    }
    // A pool that exists on some contract is named as not signed by its
    // owner; one that exists on none, as missing.
    let (on_chain, signed_by) = owners
        .iter()
        .copied()
        .find(|(on_chain, _)| !on_chain.is_zero())
        .unwrap_or_default();
    Err(CapabilityPoolError::NoMatch(
        owner_matches(on_chain, signed_by, grant.pool_id)
            .err()
            .unwrap_or_default(),
    ))
}

/// The on-chain owner of `pool_id` on `pool`'s contract (`getPool`), read
/// over `pool`'s RPC. The zero address when the pool does not exist there.
///
/// # Errors
///
/// The RPC URL is malformed, or the read fails.
pub(crate) async fn read_pool_owner(
    pool: &PaymentPoolChain,
    pool_id: PoolId,
) -> anyhow::Result<Address> {
    let rpc = decdn_client::provider::build_read_provider(&pool.rpc_url)?;
    Ok(PaymentPool::new(pool.deployment.payment_pool, rpc)
        .getPool(pool_id)
        .call()
        .await?
        .owner)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
