//! Anvil time control: advance the chain clock across dispute / compliance /
//! timelock / unbond windows so journeys don't wait wall-clock seconds.

use alloy::providers::Provider;
use anyhow::Context;

/// Advance the chain clock by `secs` and mine a block so the new timestamp
/// takes effect (`evm_increaseTime` alone is not observable until the next
/// block).
pub async fn increase_time<P: Provider>(provider: &P, secs: u64) -> anyhow::Result<()> {
    let _: serde_json::Value = provider
        .raw_request("evm_increaseTime".into(), (secs,))
        .await
        .context("evm_increaseTime")?;
    mine(provider).await
}

/// Mine a single block.
pub async fn mine<P: Provider>(provider: &P) -> anyhow::Result<()> {
    let _: serde_json::Value = provider
        .raw_request("evm_mine".into(), ())
        .await
        .context("evm_mine")?;
    Ok(())
}

/// Advance the chain clock so `block.timestamp >= ready_at`, then mine. A
/// no-op (still mines once) if the head is already past `ready_at`. Useful for
/// timelock `readyAt` gates (namespace transfer, origin-assignment activation).
pub async fn advance_to<P: Provider>(provider: &P, ready_at: u64) -> anyhow::Result<()> {
    let head = provider
        .get_block(alloy::eips::BlockId::latest())
        .await
        .context("get latest block")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?
        .header
        .timestamp;
    if ready_at > head {
        increase_time(provider, ready_at - head + 1).await
    } else {
        mine(provider).await
    }
}
