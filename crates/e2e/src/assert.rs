//! On-chain assertion helpers: typed reads of the state journeys check
//! (channel records, operator bond activity) and typed revert matching.
//! Daemon-side assertions go through the admin RPC client on
//! [`crate::node::NodeFixture`]; delivered-bytes assertions come from
//! [`crate::client::FetchOutcome`].

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolError;
use anyhow::Context;

use crate::bindings::{CapacityBond, PaymentChannel, PaymentChannelOpen};

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

/// Full on-chain `Channel` record (reuses the production seller-path binding's
/// struct, the single source of truth for the ABI layout).
pub async fn read_channel<P: Provider>(
    provider: &P,
    payment_channel: Address,
    channel_id: B256,
) -> anyhow::Result<PaymentChannel::Channel> {
    PaymentChannel::new(payment_channel, provider)
        .getChannel(channel_id)
        .call()
        .await
        .context("getChannel")
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

/// `channelId = keccak256(abi.encodePacked(client, provider, channelNonce))`,
/// matching `PaymentChannel.openChannel`. Lets a caller derive the id without
/// parsing the `ChannelOpened` receipt.
#[must_use]
pub fn channel_id(client: Address, provider: Address, channel_nonce: u64) -> B256 {
    let mut packed = Vec::with_capacity(72);
    packed.extend_from_slice(client.as_slice());
    packed.extend_from_slice(provider.as_slice());
    packed.extend_from_slice(&U256::from(channel_nonce).to_be_bytes::<32>());
    alloy::primitives::keccak256(&packed)
}

/// The next channel nonce `openChannel` will assign to `client` (the public
/// mapping getter), so a test can derive the resulting `channelId` ahead of the
/// open.
pub async fn client_channel_nonce<P: Provider>(
    provider: &P,
    payment_channel: Address,
    client: Address,
) -> anyhow::Result<U256> {
    PaymentChannelOpen::new(payment_channel, provider)
        .clientChannelNonce(client)
        .call()
        .await
        .context("clientChannelNonce")
}

/// Assert an alloy contract call reverted with exactly `E`, matching on the
/// 4-byte selector. Distinguishes the guard under test from a transport fault
/// (no revert data at all) and from a *different* revert — both of which an
/// `is_err()` check would happily accept.
pub fn expect_revert<T, E: alloy::sol_types::SolError>(
    result: Result<T, alloy::contract::Error>,
    what: &str,
) -> anyhow::Result<()> {
    let Err(err) = result else {
        anyhow::bail!("{what} must revert with {}, but succeeded", E::SIGNATURE)
    };
    let data = err.as_revert_data().with_context(|| {
        format!(
            "{what}: expected a {} revert, got no revert data: {err}",
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
        alloy::hex::encode(&data)
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn channel_id_matches_packed_keccak() {
        // `keccak256(abi.encodePacked(client, provider, uint256(nonce)))` — the
        // derivation `PaymentChannel.openChannel` uses. `expected` is an
        // independent Foundry-produced vector (not recomputed from this Rust
        // code), so a wrong *original* packing — field order, endianness, nonce
        // width — is caught, not just a later refactor. The packed preimage is
        // 20-byte client ‖ 20-byte provider ‖ 32-byte big-endian uint256(7);
        // regenerate the expected hash with:
        //   cast keccak 0x0000000000000000000000000000000000000001\
        //                 0000000000000000000000000000000000000002\
        //                 0000000000000000000000000000000000000000000000000000000000000007
        let client: Address = "0x0000000000000000000000000000000000000001"
            .parse()
            .unwrap();
        let provider: Address = "0x0000000000000000000000000000000000000002"
            .parse()
            .unwrap();
        let expected = alloy::primitives::b256!(
            "0xf8ca1ac6826b46e04989b1fb54c7e400ae0d611a635ab73fa7a95b28d5f46eda"
        );
        assert_eq!(channel_id(client, provider, 7), expected);
    }
}
