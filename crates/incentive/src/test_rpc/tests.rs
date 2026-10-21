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

/// A scripted send failure reaches the caller as the HTTP error, marked as a
/// send-leg failure, and the send after it answers as usual.
#[tokio::test]
async fn a_scripted_send_failure_fails_one_send_in_transport() {
    use alloy::rpc::types::TransactionRequest;

    let provider = ScriptedRpc::parked_receipt()
        .failing_sends([(413, String::new())])
        .provider();
    let tx = TransactionRequest::default().nonce(1).gas_limit(21_000);

    let err = provider
        .send_transaction(tx.clone())
        .await
        .expect_err("the first send fails");
    let failure = crate::tx::SendLegFailure::from_transport_error(&err)
        .expect("the failure is marked as a send-leg failure");
    let status = match failure.inner() {
        alloy::transports::RpcError::Transport(kind) => kind.as_http_error().map(|h| h.status),
        _ => None,
    };
    assert_eq!(status, Some(413), "the failure is the HTTP 413");

    let pending = provider
        .send_transaction(tx)
        .await
        .expect("the second send answers");
    assert_eq!(*pending.tx_hash(), super::SCRIPTED_TX);
}
