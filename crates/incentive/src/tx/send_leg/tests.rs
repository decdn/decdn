use super::*;
use alloy::primitives::Address;
use alloy::providers::mock::Asserter;
use alloy::providers::{Identity, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;

/// A filler-free Ethereum builder; the network must be named, since a bare
/// `send_transaction` does not infer it.
fn builder() -> ProviderBuilder<Identity, Identity, Ethereum> {
    ProviderBuilder::default()
}

/// A request a filler-free provider submits as one `eth_sendTransaction`.
fn request() -> TransactionRequest {
    TransactionRequest::default().to(Address::repeat_byte(0x01))
}

/// Send [`request`] through a provider built on `asserter` with the layer,
/// as a contract call's `send()` would, and return its failure.
async fn send_layered(asserter: Asserter) -> ContractError {
    let provider = builder()
        .layer(SendLegLayer)
        .connect_mocked_client(asserter);
    ContractError::TransportError(provider.send_transaction(request()).await.unwrap_err())
}

/// A send that fails with no JSON-RPC answer (here: an empty mock queue) is
/// marked, and the marked error prints as the unmarked one does, in its
/// `Display` and through the operator-facing renders.
#[tokio::test]
async fn a_failed_send_request_is_marked_and_prints_unchanged() {
    let marked = send_layered(Asserter::new()).await;
    let failure = SendLegFailure::from_error(&marked).expect("the send failed in transport");
    assert!(
        failure.to_string().contains("eth_sendTransaction"),
        "{failure}"
    );

    let bare = builder().connect_mocked_client(Asserter::new());
    let bare = ContractError::TransportError(bare.send_transaction(request()).await.unwrap_err());
    assert!(SendLegFailure::from_error(&bare).is_none());
    assert_eq!(marked.to_string(), bare.to_string());
    assert_eq!(
        decdn_common::redact::sanitize_error_sources(&marked),
        decdn_common::redact::sanitize_error_sources(&bare)
    );
    // The operator-facing render (`decdn` prints every failure through it)
    // folds a layer that repeats its source, as alloy's own custom transport
    // error already does, so the marker adds no line.
    assert_eq!(
        decdn_common::redact::sanitize_err_chain(&anyhow::Error::new(marked)),
        decdn_common::redact::sanitize_err_chain(&anyhow::Error::new(bare))
    );
}

/// A JSON-RPC error response to the send is the node's answer, so it passes
/// through unmarked and the message classifiers still read it.
#[tokio::test]
async fn a_rejected_send_passes_through_unmarked() {
    let asserter = Asserter::new();
    asserter.push_failure_msg("replacement transaction underpriced");
    let err = send_layered(asserter).await;
    assert!(SendLegFailure::from_error(&err).is_none(), "{err}");
    assert!(crate::tx::is_nonce_collision(&err), "{err}");
}

/// Every other send entry point alloy forwards is marked too.
#[tokio::test]
async fn the_other_send_entry_points_are_marked() {
    let provider = builder()
        .layer(SendLegLayer)
        .connect_mocked_client(Asserter::new());
    let raw = [0u8; 4];
    let sync = provider.send_transaction_sync(request()).await.unwrap_err();
    let raw_send = provider.send_raw_transaction(&raw).await.unwrap_err();
    let raw_sync = provider.send_raw_transaction_sync(&raw).await.unwrap_err();
    let conditional = provider
        .send_raw_transaction_conditional(&raw, TransactionConditional::default())
        .await
        .unwrap_err();
    for err in [sync, raw_send, raw_sync, conditional] {
        assert!(
            SendLegFailure::from_transport_error(&err).is_some(),
            "{err}"
        );
    }
}

/// Type erasure keeps the marker: `DynProvider` forwards the send hook.
#[tokio::test]
async fn an_erased_provider_still_marks() {
    let provider = builder()
        .layer(SendLegLayer)
        .connect_mocked_client(Asserter::new())
        .erased();
    let err =
        ContractError::TransportError(provider.send_transaction(request()).await.unwrap_err());
    assert!(SendLegFailure::from_error(&err).is_some(), "{err}");
}

/// A marked error without the `ContractError` wrapper is not read as one,
/// and a plain custom transport error is not a marker.
#[test]
fn only_a_marked_transport_error_is_a_send_leg_failure() {
    let plain = ContractError::TransportError(TransportErrorKind::custom_str("connection reset"));
    assert!(SendLegFailure::from_error(&plain).is_none());
    assert!(SendLegFailure::from_error(&ContractError::ContractNotDeployed).is_none());
    let marked = ContractError::TransportError(SendLegFailure::mark(
        TransportErrorKind::custom_str("connection reset"),
    ));
    assert_eq!(
        SendLegFailure::from_error(&marked)
            .unwrap()
            .inner()
            .to_string(),
        "connection reset"
    );
}
