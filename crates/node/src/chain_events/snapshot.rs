//! The block an enumeration pins its `eth_call` reads to.
//!
//! An enumeration that pages a swap-and-pop set must read every page and the
//! count it checks them against at one block, or a removal between two reads
//! can move an unread element into a read slot. `snapshot_block` picks that
//! block for the registry, slash and origin-seat snapshots.

use alloy::eips::BlockId;
use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::Result;
use tracing::info;

use super::timed;

/// How far below the reported head an enumeration snapshot pins its reads.
///
/// A load-balanced RPC reports the head of its freshest upstream, then routes
/// each call to any upstream. An `eth_call` pinned to that exact block fails on
/// every upstream that has not reached it yet, while an older block resolves on
/// all of them. So the reported head is not a block every upstream can serve.
/// 256 blocks is about 64s on Arbitrum, above the 40–200 block upstream lag seen
/// behind a hosted balancer (#2164).
///
/// The margin is a block count, sized for Arbitrum's ~250ms blocks and a
/// provider that serves state that far back. A full node that prunes state
/// within 256 blocks (geth keeps 128) answers every pinned read with a
/// transient error, so a boot against one spends its retry budget.
pub const SNAPSHOT_LAG_MARGIN_BLOCKS: u64 = 256;

/// The block an enumeration of `contract` pins its reads to:
/// [`SNAPSHOT_LAG_MARGIN_BLOCKS`] below the head.
///
/// A contract with no code at that block was deployed inside the margin — a
/// fresh local chain, or a boot right after a deploy. Reads pinned there would
/// decode an empty return, so the snapshot takes the head instead. That head pin
/// is exposed to upstream lag again; a failed read retries, and the lagged pin
/// returns once the chain is `SNAPSHOT_LAG_MARGIN_BLOCKS` past the deploy.
///
/// `Err` keeps the provider's typed cause in its chain, so a boot read can
/// classify it (`boot_retry::is_permanent_boot_error`).
pub(crate) async fn snapshot_block<P: Provider>(provider: &P, contract: Address) -> Result<u64> {
    let head = timed(None, "eth_blockNumber", provider.get_block_number()).await?;
    let lagged = head.saturating_sub(SNAPSHOT_LAG_MARGIN_BLOCKS);
    let code = timed(
        None,
        "eth_getCode",
        provider
            .get_code_at(contract)
            .block_id(BlockId::number(lagged)),
    )
    .await?;
    if code.is_empty() {
        info!(
            %contract,
            lagged,
            head,
            "no contract code at the lagged snapshot block; pinning the snapshot at head"
        );
        return Ok(head);
    }
    Ok(lagged)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
