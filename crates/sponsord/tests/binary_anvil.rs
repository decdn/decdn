//! The `sponsord` binary against a local anvil chain: it loads the treasury
//! keystore, passes the boot owner check, tops up a pool below low water,
//! issues a capability, reports all of it on `/metrics`, and exits cleanly
//! on SIGTERM.
//!
//! Requires `anvil` + `forge` on PATH: `cargo test -p sponsord --features anvil-e2e`.
#![cfg(all(unix, feature = "anvil-e2e"))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U256;
use decdn_client::buyer_pool::{ensure_allowance, open_pool};
use decdn_e2e::chain::ChainFixture;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{CapabilityGrant, Deployment, eth_identity, voucher_domain};

/// The daemon process, killed and reaped if the test ends before it exits on
/// its own, so a failed assertion never leaves a daemon running.
struct Daemon(Option<std::process::Child>);

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";
/// Opened below the daemon's low-water mark, so the keeper tops it up.
const OPEN_DEPOSIT: u64 = 1_000_000;
const LOW_WATER: u64 = 2_000_000;
const REFILL: u64 = 3_000_000;

async fn metrics(base: &str) -> String {
    reqwest::get(format!("{base}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test]
async fn daemon_binary_serves_tops_up_and_shuts_down() {
    let chain = ChainFixture::launch().await.expect("launch");

    // The treasury wallet, as an encrypted keystore the binary will load.
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    eth_identity::generate_and_persist(dir.path(), "pw", false).unwrap();
    let keystore = eth_identity::keystore_path(dir.path());
    let signer = eth_identity::load_signer(&keystore, "pw").unwrap();
    let owner = signer.address();
    chain.fund_eth(owner, 100).await.unwrap();
    chain
        .mint_usdc(owner, U256::from(OPEN_DEPOSIT + 10 * REFILL))
        .await
        .unwrap();
    let provider = chain.provider_for(&signer);
    let pool_addr = chain.addrs().payment_pool;
    ensure_allowance(
        &provider,
        chain.usdc(),
        owner,
        pool_addr,
        Some(U256::from(OPEN_DEPOSIT)),
    )
    .await
    .unwrap();
    let opened = open_pool(
        &PaymentPool::new(pool_addr, provider),
        Arc::new(signer.clone()),
        Deployment {
            chain_id: chain.chain_id(),
            payment_pool: pool_addr,
        },
        chain.usdc(),
        owner,
        U256::from(OPEN_DEPOSIT),
    )
    .await
    .unwrap();
    let pool_id = opened.state.pool_id;

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let base = format!("http://127.0.0.1:{port}");
    let mut daemon = Daemon(Some(
        Command::new(env!("CARGO_BIN_EXE_sponsord"))
            .env_clear()
            .env("SPONSORD_BIND", format!("127.0.0.1:{port}"))
            .env("SPONSORD_API_TOKEN", TOKEN)
            .env("SPONSORD_RPC_URL", chain.rpc_url())
            .env("SPONSORD_CHAIN_ID", chain.chain_id().to_string())
            .env("SPONSORD_PAYMENT_POOL_ADDR", pool_addr.to_string())
            .env("SPONSORD_POOL_ID", pool_id.to_string())
            .env("SPONSORD_TREASURY_KEYSTORE", &keystore)
            .env("SPONSORD_TREASURY_PASSWORD", "pw")
            .env("SPONSORD_POOL_LOW_WATER_MICRO_USDC", LOW_WATER.to_string())
            .env("SPONSORD_POOL_REFILL_MICRO_USDC", REFILL.to_string())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    ));
    let pid = daemon.0.as_ref().unwrap().id();

    // Up, and the keeper's first check has topped the pool up.
    let mut text = String::new();
    for _ in 0..200 {
        if let Ok(resp) = reqwest::get(format!("{base}/metrics")).await {
            text = resp.text().await.unwrap();
            if text.contains("sponsord_pool_topups_total 1") {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(text.contains("sponsord_pool_topups_total 1"), "{text}");
    assert!(
        text.contains(&format!(
            "sponsord_pool_remaining_micro_usdc {}",
            OPEN_DEPOSIT + REFILL
        )),
        "{text}"
    );

    // A capability, signed by the treasury key for the pool.
    let delegate = "0x00000000000000000000000000000000000000aa";
    let issued: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/v1/capabilities"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({"signer": delegate}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let grant = CapabilityGrant::from_token(issued["token"].as_str().unwrap()).unwrap();
    assert_eq!(
        grant
            .owner(&voucher_domain(chain.chain_id(), pool_addr))
            .unwrap(),
        owner
    );
    assert!(
        metrics(&base)
            .await
            .contains("sponsord_capabilities_issued_total{registered=\"false\"} 1")
    );

    // SIGTERM: a clean exit.
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let mut child = daemon.0.take().unwrap();
    let exit = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.success(), "{exit}");
}
