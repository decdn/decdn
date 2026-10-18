use std::time::Duration;

use alloy::eips::BlockId;
use alloy::providers::Provider;

use super::ScriptedRpc;

/// A scripted head decodes through the provider's block read at the
/// timestamp set. A caller's chain-time gate is only exercised when this
/// holds; a decode failure would leave an optimistic caller on its fallback
/// path without a test noticing.
#[tokio::test]
async fn a_scripted_head_decodes_at_its_timestamp() {
    let provider = ScriptedRpc::parked_receipt().at_head(7).provider();
    let head = provider
        .get_block(BlockId::latest())
        .await
        .expect("the scripted head decodes")
        .expect("the scripted head is present");
    assert_eq!(head.header.timestamp, 7);
}

/// With no head set, a block read parks like every other unscripted call.
#[tokio::test(start_paused = true)]
async fn a_block_read_with_no_head_set_parks() {
    let provider = ScriptedRpc::parked_receipt().provider();
    let read = tokio::time::timeout(
        Duration::from_secs(60),
        provider.get_block(BlockId::latest()),
    )
    .await;
    assert!(read.is_err(), "the block read must still be pending");
}
