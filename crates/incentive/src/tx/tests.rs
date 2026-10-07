use super::*;

/// A JSON-RPC error *response* — the node answered, rejecting the call.
/// Built by deserializing the wire shape, since `ErrorPayload` is not
/// re-exported through `alloy::transports` (only `RpcError` is).
fn error_resp(code: i64, message: &str) -> ContractError {
    ContractError::TransportError(alloy::transports::RpcError::ErrorResp(
        serde_json::from_value(serde_json::json!({ "code": code, "message": message })).unwrap(),
    ))
}

/// A same-account nonce collision is a JSON-RPC rejection with one of the two
/// collision messages (any case); an unrelated rejection, a revert, or a
/// transport failure is not.
#[test]
fn only_a_nonce_rejection_is_a_nonce_collision() {
    assert!(super::is_nonce_collision(&error_resp(
        -32003,
        "nonce too low"
    )));
    assert!(super::is_nonce_collision(&error_resp(
        -32000,
        "Nonce too low: next nonce 7, tx nonce 6"
    )));
    assert!(super::is_nonce_collision(&error_resp(
        -32000,
        "replacement transaction underpriced"
    )));
    assert!(!super::is_nonce_collision(&error_resp(
        3,
        "execution reverted"
    )));
    assert!(!super::is_nonce_collision(&error_resp(
        -32000,
        "insufficient funds for gas"
    )));
    assert!(!super::is_nonce_collision(&ContractError::TransportError(
        alloy::transports::TransportErrorKind::custom_str("nonce too low")
    )));
}

/// The tx hash reaches the receipt on every arm that has one, and only
/// those; `maybe_effected` splits the "may be pending" arms from the
/// definitively-nothing arms.
#[test]
fn send_outcome_hash_and_effect_are_total() {
    let h = B256::repeat_byte(0x55);
    assert_eq!(SendOutcome::Confirmed(h).tx(), Some(h));
    assert_eq!(SendOutcome::Reverted(h).tx(), Some(h));
    assert_eq!(SendOutcome::InFlight(h).tx(), Some(h));
    assert_eq!(SendOutcome::MaybeBroadcast.tx(), None);
    assert_eq!(SendOutcome::Rejected.tx(), None);
    assert_eq!(SendOutcome::NotSent.tx(), None);

    assert!(SendOutcome::Confirmed(h).maybe_effected());
    assert!(SendOutcome::InFlight(h).maybe_effected());
    // The #1577 case: a transport failure may have broadcast the tx, so it
    // must NOT be reported as "nothing happened".
    assert!(SendOutcome::MaybeBroadcast.maybe_effected());
    // A confirmed revert had no effect even though it kept its hash (#1550).
    assert!(!SendOutcome::Reverted(h).maybe_effected());
    assert!(!SendOutcome::Rejected.maybe_effected());
    assert!(!SendOutcome::NotSent.maybe_effected());
}

/// An HTTP transport error carrying `status`, for the 4xx/5xx split.
fn http_error(status: u16) -> ContractError {
    ContractError::TransportError(alloy::transports::RpcError::Transport(
        alloy::transports::TransportErrorKind::HttpError(alloy::transports::HttpError {
            status,
            body: String::new(),
        }),
    ))
}

/// Failures that broadcast nothing: a JSON-RPC error *response* (the node
/// answered and rejected), a client-side fault that never left the process,
/// and an HTTP 4xx where the gateway refused the request. Re-running any of
/// them is safe, so none may warn about a possibly-broadcast transaction.
#[test]
fn a_rejection_is_not_a_broadcast() {
    // The modal case: a pre-flight gas-estimate revert.
    assert!(!send_broadcast_unknown(&error_resp(
        3,
        "execution reverted"
    )));
    assert!(!send_broadcast_unknown(&error_resp(
        -32000,
        "nonce too low"
    )));
    // Local faults raised before the request leaves the client — these must
    // NOT be lumped into the uncertain bucket by an `as_error_resp()` shortcut.
    assert!(!send_broadcast_unknown(&ContractError::TransportError(
        alloy::transports::RpcError::UnsupportedFeature("batching")
    )));
    // A non-transport contract error is local too.
    assert!(!send_broadcast_unknown(&ContractError::ContractNotDeployed));
    // HTTP 4xx: the gateway refused the request (bad path, expired key).
    assert!(!send_broadcast_unknown(&http_error(401)));
    assert!(!send_broadcast_unknown(&http_error(404)));
}

/// A timeout / reset / 5xx / undeserializable body leaves the tx's fate
/// unknown — it may be in the mempool. Classifying this as a clean rejection
/// is the #1577 bug.
#[test]
fn a_transport_failure_leaves_the_broadcast_unknown() {
    let transport = ContractError::TransportError(
        alloy::transports::TransportErrorKind::custom_str("connection reset"),
    );
    assert!(send_broadcast_unknown(&transport));
    // A null/absent response is transport-shaped too: no answer came back.
    assert!(send_broadcast_unknown(&ContractError::TransportError(
        alloy::transports::RpcError::NullResp
    )));
    // HTTP 5xx: the node may have broadcast before the load balancer failed.
    assert!(send_broadcast_unknown(&http_error(502)));
    assert!(send_broadcast_unknown(&http_error(500)));
}
