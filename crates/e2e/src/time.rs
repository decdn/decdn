//! Anvil time control: advance the chain clock across dispute / compliance /
//! timelock windows so journeys don't wait wall-clock seconds.

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

/// Mine `count` blocks in one `anvil_mine` call.
pub async fn mine_blocks<P: Provider>(provider: &P, count: u64) -> anyhow::Result<()> {
    let _: serde_json::Value = provider
        .raw_request("anvil_mine".into(), (count,))
        .await
        .context("anvil_mine")?;
    Ok(())
}

/// Mine past a node's snapshot lag margin: a node pins its registry and seat
/// snapshots `SNAPSHOT_LAG_MARGIN_BLOCKS` below the head, so a daemon
/// that boots after this call sees every transaction mined before it. Each
/// block advances the chain clock by one second, as `anvil_mine` does.
pub async fn mine_past_snapshot_margin<P: Provider>(provider: &P) -> anyhow::Result<()> {
    mine_blocks(
        provider,
        decdn_node::chain_events::snapshot::SNAPSHOT_LAG_MARGIN_BLOCKS + 1,
    )
    .await
    .context("mine past the snapshot lag margin")
}
