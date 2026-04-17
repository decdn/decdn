//! Integration tests for the loopback admin HTTP surface (ADR 025).
//!
//! Spawns the admin server directly against a seeded `PeerTable` and
//! verifies the `GET /v1/peers` route round-trips through real HTTP.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use decdn_gossip::PeerTable;
use decdn_node::admin::{self, AdminState};
use decdn_protocol::{LoadHint, NodeAnnounce, NodeAnnounceBody};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, oneshot};

fn mk_announce(node_id: [u8; 32], region: &str, ts_us: u64) -> NodeAnnounce {
    NodeAnnounce {
        body: NodeAnnounceBody {
            node_id,
            region: region.to_string(),
            load: LoadHint {
                active_streams: 0,
                bandwidth_utilization: 0,
            },
            popular_hashes: vec![],
            timestamp_us: ts_us,
        },
        signature: vec![0u8; 64],
    }
}

async fn bind_loopback() -> anyhow::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    Ok((listener, addr))
}

#[tokio::test]
async fn admin_peers_returns_empty_before_any_announce() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let state = AdminState::new(peer_table);

    let (listener, addr) = bind_loopback().await?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        admin::serve(listener, state, stop_rx).await.ok();
    });

    let client = reqwest::Client::new();
    let resp = client.get(format!("http://{addr}/v1/peers")).send().await?;
    assert!(resp.status().is_success(), "status: {}", resp.status());
    let body = resp.text().await?;
    assert_eq!(body, r#"{"peers":[]}"#);

    let _ = stop_tx.send(());
    server.await?;
    Ok(())
}

#[tokio::test]
async fn admin_peers_lists_seeded_entries_sorted_desc() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    {
        let mut guard = peer_table.write().await;
        // (id, region, announce_ts_us, now_us)
        guard
            .insert_or_refresh(mk_announce([1u8; 32], "US", 1), 500)
            .ok();
        guard
            .insert_or_refresh(mk_announce([2u8; 32], "EU", 2), 700)
            .ok();
        guard
            .insert_or_refresh(mk_announce([3u8; 32], "AP", 3), 600)
            .ok();
    }
    let state = AdminState::new(Arc::clone(&peer_table));

    let (listener, addr) = bind_loopback().await?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        admin::serve(listener, state, stop_rx).await.ok();
    });

    let resp = reqwest::get(format!("http://{addr}/v1/peers")).await?;
    assert!(resp.status().is_success());
    let json: serde_json::Value = resp.json().await?;
    let peers = json["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 3);

    // Sorted by last_seen_us descending (700, 600, 500).
    let last_seens: Vec<u64> = peers
        .iter()
        .map(|p| p["last_seen_us"].as_u64().unwrap_or_default())
        .collect();
    assert_eq!(last_seens, vec![700, 600, 500]);

    // Regions follow the same order.
    let regions: Vec<&str> = peers
        .iter()
        .map(|p| p["region"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(regions, vec!["EU", "AP", "US"]);

    // Node IDs are lowercase hex, 64 chars each.
    for p in peers {
        let id = p["node_id"].as_str().expect("node_id string");
        assert_eq!(id.len(), 64);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    let _ = stop_tx.send(());
    server.await?;
    Ok(())
}

#[tokio::test]
async fn admin_rejects_unknown_route_with_404() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let state = AdminState::new(peer_table);

    let (listener, addr) = bind_loopback().await?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        admin::serve(listener, state, stop_rx).await.ok();
    });

    let resp = reqwest::get(format!("http://{addr}/v1/nonexistent")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let _ = stop_tx.send(());
    server.await?;
    Ok(())
}

#[tokio::test]
async fn admin_rejects_post_to_peers_with_405() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let state = AdminState::new(peer_table);

    let (listener, addr) = bind_loopback().await?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        admin::serve(listener, state, stop_rx).await.ok();
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/v1/peers"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        resp.headers().get("allow").and_then(|v| v.to_str().ok()),
        Some("GET")
    );

    let _ = stop_tx.send(());
    server.await?;
    Ok(())
}

#[tokio::test]
async fn admin_shutdown_closes_listener() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let state = AdminState::new(peer_table);

    let (listener, addr) = bind_loopback().await?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        admin::serve(listener, state, stop_rx).await.ok();
    });

    // Baseline: server is up.
    reqwest::get(format!("http://{addr}/v1/peers")).await?;

    let _ = stop_tx.send(());
    server.await?;

    // After shutdown the port should refuse new connections fairly quickly.
    // Bound the wait so a stuck test fails instead of hanging.
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reqwest::get(format!("http://{addr}/v1/peers")),
    )
    .await;
    match err {
        Ok(Ok(_)) => panic!("admin server still responding after shutdown"),
        // Either a reqwest error or the outer timeout are acceptable — both
        // mean "no longer serving".
        Ok(Err(_)) | Err(_) => Ok(()),
    }
}
