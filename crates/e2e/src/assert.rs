//! On-chain assertion helpers: typed reads of the state journeys check
//! (pool records, per-lane watermarks, capability authorizations, operator bond
//! activity) and typed revert matching. Daemon-side assertions go through the
//! admin RPC client on [`crate::node::NodeFixture`]; delivered-bytes assertions
//! come from [`crate::client::FetchOutcome`].

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolError;
use anyhow::Context;

use crate::bindings::{CapacityBond, PaymentPool};

/// Assert an alloy contract call reverted with exactly `E`, matching on the
/// 4-byte selector. Distinguishes the guard under test from a transport fault
/// (no revert data at all) and from a *different* revert — both of which an
/// `is_err()` check would happily accept.
pub fn expect_revert<T, E: SolError>(
    result: Result<T, alloy::contract::Error>,
    what: &str,
) -> anyhow::Result<()> {
    match result {
        Ok(_) => anyhow::bail!("{what} must revert with {}, but succeeded", E::SIGNATURE),
        Err(err) => assert_revert_data::<E>(err.as_revert_data(), what, &err),
    }
}

/// [`expect_revert`] for a call whose typed error has already been wrapped in an
/// `anyhow` chain — the shape every `ChainFixture` write helper returns.
///
/// `anyhow`'s `.context()` preserves the source, so the typed
/// `alloy::contract::Error` is still reachable by downcast and its revert data
/// can be matched on `E::SELECTOR`. That is what keeps the assertion pinned to
/// the ABI rather than to a hand-copied selector literal or to whatever
/// `ErrorPayload::Display` happens to render (#1042).
pub fn expect_revert_anyhow<E: SolError>(err: &anyhow::Error, what: &str) -> anyhow::Result<()> {
    let data = err
        .chain()
        .find_map(|source| source.downcast_ref::<alloy::contract::Error>())
        .and_then(alloy::contract::Error::as_revert_data);
    assert_revert_data::<E>(data, what, err)
}

/// Shared selector check. `rendered` is only used to build the failure message.
fn assert_revert_data<E: SolError>(
    data: Option<alloy::primitives::Bytes>,
    what: &str,
    rendered: &dyn std::fmt::Display,
) -> anyhow::Result<()> {
    let data = data.with_context(|| {
        format!(
            "{what}: expected a {} revert, got no revert data: {rendered}",
            E::SIGNATURE
        )
    })?;
    let selector = data
        .get(..4)
        .context("revert payload too short to carry a selector")?;
    anyhow::ensure!(
        selector == E::SELECTOR,
        "{what}: expected {}, got revert data 0x{}",
        E::SIGNATURE,
        alloy::hex::encode(data)
    );
    Ok(())
}

/// Full on-chain `Pool` record (reuses the production pool binding's struct, the
/// single source of truth for the ABI layout).
pub async fn read_pool<P: Provider>(
    provider: &P,
    payment_pool: Address,
    pool_id: B256,
) -> anyhow::Result<PaymentPool::Pool> {
    PaymentPool::new(payment_pool, provider)
        .getPool(pool_id)
        .call()
        .await
        .context("getPool")
}

/// A signer's `(cap, expiry, spent)` authorization against a pool. A zero `cap`
/// and zero `expiry` means the signer is not yet registered — the pre-condition
/// a first redemption clears when it registers the owner-signed capability.
pub async fn read_authorization<P: Provider>(
    provider: &P,
    payment_pool: Address,
    pool_id: B256,
    signer: Address,
) -> anyhow::Result<PaymentPool::Authorization> {
    PaymentPool::new(payment_pool, provider)
        .getAuthorization(pool_id, signer)
        .call()
        .await
        .context("getAuthorization")
}

/// A `(signer, provider)` lane's cumulative-paid `(amount, bytesDelivered)`
/// watermark — the on-chain figure a `PoolRedeemed` advances.
pub async fn read_watermark<P: Provider>(
    provider: &P,
    payment_pool: Address,
    pool_id: B256,
    signer: Address,
    provider_addr: Address,
) -> anyhow::Result<PaymentPool::Lane> {
    PaymentPool::new(payment_pool, provider)
        .getWatermark(pool_id, signer, provider_addr)
        .call()
        .await
        .context("getWatermark")
}

/// Whether `operator` is on-chain `isActive` (bonded + registered).
pub async fn operator_active<P: Provider>(
    provider: &P,
    capacity_bond: Address,
    operator: Address,
) -> anyhow::Result<bool> {
    CapacityBond::new(capacity_bond, provider)
        .isActive(operator)
        .call()
        .await
        .context("isActive")
}

/// `poolId = keccak256(abi.encodePacked(owner, ownerPoolNonce))`, matching
/// `PaymentPool.openPool`. Lets a caller derive the id without parsing the
/// `PoolOpened` receipt.
#[must_use]
pub fn pool_id(owner: Address, owner_pool_nonce: u64) -> B256 {
    let mut packed = Vec::with_capacity(52);
    packed.extend_from_slice(owner.as_slice());
    packed.extend_from_slice(&U256::from(owner_pool_nonce).to_be_bytes::<32>());
    alloy::primitives::keccak256(&packed)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
