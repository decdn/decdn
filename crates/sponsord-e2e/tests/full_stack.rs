//! `decdn-sponsored pull` end to end: the CLI against a real onramp served
//! over HTTP, in front of a real daemon, with a stub `decdn` binary. A
//! background task plays the person in the browser: once the CLI has made
//! its download key, it posts `/v1/fund` for it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use decdn_incentive::{CapabilityGrant, voucher_domain};
use decdn_sponsored::config::Config;
use sponsord::http::ApiState;
use sponsord_api::client::DaemonClient;
use sponsord_core::test_support::{TEST_CHAIN_ID, TEST_PAYMENT_POOL, fake_sponsor};
use sponsord_onramp::test_support::{FakeGate, test_config};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";
const HASH: &str = "9194000d7b356650e6924a7746ec4afb0b705838b65913c0e46cba6b15af69e5";

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    url
}

/// A stub `decdn` that appends its argv to `<dir>/calls` and exits 0.
#[cfg(unix)]
fn stub_decdn(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("decdn");
    std::fs::write(
        &bin,
        format!(
            "#!/bin/sh\necho \"$@\" >> '{}'\nexit 0\n",
            dir.join("calls").display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

/// A stub `decdn` that appends its argv to `<dir>/calls` and exits 0.
#[cfg(windows)]
fn stub_decdn(dir: &Path) -> PathBuf {
    let bin = dir.join("decdn.cmd");
    std::fs::write(
        &bin,
        format!(
            "@echo off\r\necho %*>> \"{}\"\r\nexit /b 0\r\n",
            dir.join("calls").display()
        ),
    )
    .unwrap();
    bin
}

/// Wait for the CLI to write its download key's address, then pass the gate
/// for it as the browser would.
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
async fn pull_gets_a_capability_through_the_gate_and_runs_decdn() {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    let daemon_url = serve(sponsord::http::router(ApiState::new(
        Arc::new(sponsor),
        TOKEN.into(),
    )))
    .await;

    let cfg = test_config();
    let daemon = Arc::new(DaemonClient::new(
        &daemon_url,
        TOKEN.into(),
        reqwest::Client::new(),
    ));
    let state = sponsord_onramp::state::build(&cfg, daemon, Arc::new(FakeGate::new(true)))
        .await
        .unwrap();
    let onramp_url = serve(sponsord_onramp::http::router(state)).await;

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("sponsored");
    let state_dir = data_dir.join("downloads").join(HASH);
    let cli = Config {
        onramp_url: onramp_url.clone(),
        decdn_bin: stub_decdn(tmp.path()).display().to_string(),
        data_dir,
    };

    let browser = tokio::spawn(browser(onramp_url, state_dir.join("address")));
    tokio::time::timeout(
        Duration::from_secs(30),
        decdn_sponsored::pull::pull(HASH, &tmp.path().join("out"), None, &cli),
    )
    .await
    .expect("pull finishes")
    .unwrap();
    let signer = browser.await.unwrap();

    let calls = std::fs::read_to_string(tmp.path().join("calls")).unwrap();
    assert!(calls.starts_with(&format!("bundle pull --hash {HASH} ")));
    assert!(calls.contains(&format!("--payment-pool-address {TEST_PAYMENT_POOL}")));
    assert!(calls.contains(&format!("--chain-id {TEST_CHAIN_ID}")));
    assert!(!state_dir.exists(), "state is discarded after a good pull");

    // The daemon signed for the CLI's key, as the pool owner.
    let token = reqwest::get(format!("{}/v1/capability?client={signer}", cli.onramp_url))
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let grant = CapabilityGrant::from_token(&token).unwrap();
    assert_eq!(grant.signer, signer);
    assert_eq!(
        grant
            .owner(&voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL))
            .unwrap(),
        sponsord_core::pool::PoolChain::owner_address(pool.as_ref())
    );
}
