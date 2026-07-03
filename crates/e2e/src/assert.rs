//! On-chain assertion helpers: typed reads of the state journeys check
//! (channel records, operator bond activity). Daemon-side assertions go through
//! the admin RPC client on [`crate::node::NodeFixture`]; delivered-bytes
//! assertions come from [`crate::client::FetchOutcome`].

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::Context;

use crate::bindings::{CapacityBond, PaymentChannel, PaymentChannelOpen};

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
