use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;

const CLIENT: Address = Address::repeat_byte(0xaa);

fn gate(server: &MockServer) -> TurnstileGate {
    TurnstileGate::new(
        "secret".into(),
        "KEY".into(),
        "Acme <Models>".into(),
        None,
        reqwest::Client::new(),
    )
    .with_endpoint(format!("{}/siteverify", server.uri()))
}

#[tokio::test]
async fn success_json_passes_and_sends_the_remote_ip() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/siteverify"))
        .and(body_string_contains("remoteip=203.0.113.7"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})),
        )
        .mount(&server)
        .await;
    let ip = Some("203.0.113.7".parse().unwrap());
    assert!(gate(&server).verify(CLIENT, "token", ip).await.unwrap());
}

#[tokio::test]
async fn failure_json_refuses() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/siteverify"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"success": false, "error-codes": ["invalid-input-response"]}),
        ))
        .mount(&server)
        .await;
    assert!(!gate(&server).verify(CLIENT, "bad", None).await.unwrap());
}

#[tokio::test]
async fn page_carries_sitekey_client_and_escaped_brand() {
    let server = MockServer::start().await;
    let html = gate(&server).page(CLIENT);
    assert!(html.contains("data-sitekey=\"KEY\""));
    assert!(html.contains(&CLIENT.to_string()));
    assert!(html.contains("Acme &lt;Models&gt;"));
    assert!(!html.contains("{{"));
    // It posts to the fund route and tells an expired key apart.
    assert!(html.contains("fetch(\"/v1/fund\""));
    assert!(html.contains("id=\"expired\"") && html.contains("409"));
}
