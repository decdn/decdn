//! A sponsored download end to end: the onramp's `decdn.sh`, as the onramp
//! serves it, against that onramp over HTTP in front of a real daemon, with
//! a stub `decdn` from a `file://` release. A background task plays the
//! person in the browser: once the script has made its download key, it
//! posts `/v1/fund` for it.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use decdn_incentive::{CapabilityGrant, voucher_domain};
use sponsord::http::ApiState;
use sponsord_api::client::DaemonClient;
use sponsord_core::test_support::{TEST_CHAIN_ID, TEST_PAYMENT_POOL, fake_sponsor};
use sponsord_onramp::test_support::{FakeGate, STUB_DECDN, publish_release, test_config};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";
const HASH: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

async fn listen() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

fn serve(listener: tokio::net::TcpListener, app: axum::Router) {
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
}

/// Wait for the script to write its download key's address, then pass the
/// gate for it as the browser would.
async fn browser(onramp: String, address_file: PathBuf) -> Address {
    let address = loop {
        if let Some(a) = std::fs::read_to_string(&address_file)
            .ok()
            .and_then(|s| s.trim().parse::<Address>().ok())
        {
            break a;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let resp = reqwest::Client::new()
        .post(format!("{onramp}/v1/fund"))
        .json(&serde_json::json!({"client": address, "proof": "ok"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{}", resp.status());
    address
}

#[tokio::test]
async fn the_installer_gets_a_capability_through_the_gate_and_runs_decdn() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    let (daemon_listener, daemon_url) = listen().await;
    serve(
        daemon_listener,
        sponsord::http::router(ApiState::new(Arc::new(sponsor), TOKEN.into())),
    );

    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let stub = tmp.path().join("stub");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&stub).unwrap();
    let releases = tmp.path().join("releases");
    let sums = publish_release(&releases, "decdn", "decdn", "v0.1.0", "0.1.0", STUB_DECDN);

    let (onramp_listener, onramp_url) = listen().await;
    let mut cfg = test_config();
    cfg.public_url.clone_from(&onramp_url);
    cfg.releases_base = format!("file://{}", releases.display());
    cfg.decdn_release.sums_sha256 = sums;
    let daemon = Arc::new(DaemonClient::new(
        &daemon_url,
        TOKEN.into(),
        reqwest::Client::new(),
    ));
    let state = sponsord_onramp::state::build(&cfg, daemon, Arc::new(FakeGate::new(true)))
        .await
        .unwrap();
    serve(onramp_listener, sponsord_onramp::http::router(state));

    let script = tmp.path().join("decdn.sh");
    let body = reqwest::get(format!("{onramp_url}/decdn.sh"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    std::fs::write(&script, body).unwrap();

    let state_dir = home.join(".decdn/sponsored/downloads").join(HASH);
    let browser = tokio::spawn(browser(onramp_url, state_dir.join("address")));
    let out = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new("sh")
            .arg(&script)
            .args(["pull", &format!("b3:{HASH}")])
            .env("HOME", &home)
            .env("STUB_DIR", &stub)
            .stdin(std::process::Stdio::null())
            .output(),
    )
    .await
    .expect("decdn.sh finishes")
    .unwrap();
    assert!(
        out.status.success(),
        "decdn.sh failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let signer = browser.await.unwrap();

    let calls = std::fs::read_to_string(stub.join("calls")).unwrap();
    assert!(calls.starts_with(&format!("bundle pull --hash {HASH} ")));
    assert!(calls.contains(&format!("--payment-pool-address {TEST_PAYMENT_POOL}")));
    assert!(calls.contains(&format!("--chain-id {TEST_CHAIN_ID}")));
    assert!(!state_dir.exists(), "state is deleted after a good pull");

    // decdn got the capability the daemon signed for the script's key, as
    // the pool owner.
    let token = calls
        .split_whitespace()
        .skip_while(|w| *w != "--capability")
        .nth(1)
        .unwrap();
    let grant = CapabilityGrant::from_token(token).unwrap();
    assert_eq!(grant.signer, signer);
    assert_eq!(
        grant
            .owner(&voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL))
            .unwrap(),
        sponsord_core::pool::PoolChain::owner_address(pool.as_ref())
    );
}
