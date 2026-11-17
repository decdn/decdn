use std::time::Duration;

use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const TX: B256 = B256::repeat_byte(0x77);

/// No pause between reads, `attempts` reads.
const fn quick(attempts: u32) -> IrisWait {
    IrisWait {
        first: Duration::ZERO,
        max: Duration::ZERO,
        attempts,
    }
}

fn pending() -> serde_json::Value {
    serde_json::json!({"messages": [{
        "message": "0x",
        "attestation": "PENDING",
        "status": "pending_confirmations",
    }]})
}

fn complete() -> serde_json::Value {
    serde_json::json!({"messages": [{
        "message": "0x0102",
        "attestation": "0xaabb",
        "status": "complete",
    }]})
}

/// The read the command makes: the source domain in the path, the transaction
/// hash in the query.
fn messages_of(server_path: &str) -> wiremock::MockBuilder {
    Mock::given(method("GET"))
        .and(path(server_path))
        .and(query_param("transactionHash", format!("{TX:#x}")))
}

/// A pending attestation is read again until it completes.
#[tokio::test]
async fn a_pending_attestation_is_read_until_complete() {
    let server = MockServer::start().await;
    messages_of("/v2/messages/3")
        .respond_with(ResponseTemplate::new(200).set_body_json(pending()))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&server)
        .await;
    messages_of("/v2/messages/3")
        .respond_with(ResponseTemplate::new(200).set_body_json(complete()))
        .with_priority(2)
        .mount(&server)
        .await;
    let status = fetch_messages(&client().unwrap(), &server.uri(), 3, TX, &quick(5))
        .await
        .unwrap();
    assert_eq!(
        status,
        MessageStatus::Complete(vec![Attested {
            message: Bytes::from(vec![1, 2]),
            attestation: Bytes::from(vec![0xaa, 0xbb]),
        }])
    );
}

/// A transaction Iris has not indexed yet (`404`) reads as pending, and a wait
/// that runs out reports pending rather than failing.
#[tokio::test]
async fn an_unknown_transaction_is_pending_until_the_wait_runs_out() {
    let server = MockServer::start().await;
    messages_of("/v2/messages/6")
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(serde_json::json!({"error": "Message not found"})),
        )
        .expect(3)
        .mount(&server)
        .await;
    let status = fetch_messages(&client().unwrap(), &server.uri(), 6, TX, &quick(3))
        .await
        .unwrap();
    assert_eq!(status, MessageStatus::Pending);
}

/// A server error is an error, never "pending".
#[tokio::test]
async fn a_server_error_fails_the_read() {
    let server = MockServer::start().await;
    messages_of("/v2/messages/0")
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let err = fetch_messages(&client().unwrap(), &server.uri(), 0, TX, &quick(3))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("500"), "{err}");
}
