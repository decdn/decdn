//! Integration tests for the loopback admin JSON-RPC surface (ADR 025).
//!
//! Spawns the admin server directly against a seeded `PeerTable` and
//! verifies `admin_v1_peersList` round-trips through real HTTP via
//! jsonrpsee's generated client bindings. Unit tests for the
//! peers-list shape itself (sort, hex encoding, field mapping) live
//! next to the server impl in `admin.rs`; this file owns the wire-
//! level checks only.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Instant;

use decdn_cache::CacheEngine;
use decdn_gossip::PeerTable;
use decdn_node::admin::{self, AdminRpcClient, AdminState, DrainTrigger};
use decdn_node::cli::{AnnounceArgs, DrainArgs, EvictArgs, HealthArgs, PeersArgs, ReloadArgs};
use decdn_node::commands;
use decdn_protocol::{LoadHint, NodeAnnounce, NodeAnnounceBody};
use jsonrpsee::core::ClientError;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
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

/// Build a throwaway cache engine for tests that don't exercise cache
/// behavior — `AdminState::new` requires one and these tests only check
/// peers / health round-tripping. The returned `TempDir` must outlive the
/// engine; callers bind it with `_tmp` to keep RAII in scope.
async fn test_cache() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), None, 1).await?;
    Ok((cache, tmp))
}

/// Spawn an admin server with the given state, returning its URL and a
/// `(stop_tx, join)` pair. The join handle must be awaited after
/// sending on `stop_tx` so the test doesn't leak a background task.
async fn spawn_admin(
    state: AdminState,
) -> anyhow::Result<(String, oneshot::Sender<()>, tokio::task::JoinHandle<()>)> {
    let (listener, addr) = bind_loopback().await?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        admin::serve(listener, state, stop_rx).await.ok();
    });
    Ok((format!("http://{addr}"), stop_tx, join))
}

#[tokio::test]
async fn peers_list_empty_peer_table() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.peers_list().await?;
    assert!(resp.peers.is_empty());

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

#[tokio::test]
async fn peers_list_seeded_entries_sorted_desc() -> anyhow::Result<()> {
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
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        Arc::clone(&peer_table),
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.peers_list().await?;
    assert_eq!(resp.peers.len(), 3);

    // Sorted by last_seen_us descending (700, 600, 500).
    let last_seens: Vec<u64> = resp.peers.iter().map(|p| p.last_seen_us).collect();
    assert_eq!(last_seens, vec![700, 600, 500]);

    // Regions follow the same order.
    let regions: Vec<&str> = resp.peers.iter().map(|p| p.region.as_str()).collect();
    assert_eq!(regions, vec!["EU", "AP", "US"]);

    // Node IDs are lowercase hex, 64 chars each.
    for p in &resp.peers {
        assert_eq!(p.node_id.len(), 64);
        assert!(
            p.node_id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

#[tokio::test]
async fn health_returns_hex_node_id_and_uptime() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let id = [0xABu8; 32];
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        id,
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.health().await?;
    assert_eq!(resp.node_id, "ab".repeat(32));
    // `uptime_s` is whole-seconds-since-`started_at`; on a fast test
    // host this is almost always 0. Just assert the type-correct
    // round-trip — `health` returning at all proves the wire format.
    assert!(
        resp.uptime_s < 60,
        "uptime_s suspiciously large: {}",
        resp.uptime_s
    );

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// JSON-RPC `-32601 Method not found` is the equivalent of the old
/// HTTP `404` for an unknown route. Confirm the server returns it for
/// a method name outside the `admin_v1_` namespace.
#[tokio::test]
async fn unknown_method_returns_method_not_found() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let result: Result<serde_json::Value, ClientError> =
        client.request("admin_v1_nonexistent", rpc_params![]).await;
    match result {
        Err(ClientError::Call(obj)) => {
            // JSON-RPC 2.0 method-not-found code.
            assert_eq!(obj.code(), -32601, "code was {}: {:?}", obj.code(), obj);
        }
        other => panic!("expected Call(-32601), got: {other:?}"),
    }

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// The CLI's connection-refused branch is the single most operator-
/// visible error path (mistyped `--admin-url`, node not running). Bind
/// a loopback socket, drop it, and call `commands::peers` against its
/// address: the kernel will return `ECONNREFUSED` and the CLI should
/// surface that with the "is the node running?" hint.
#[tokio::test]
async fn cli_peers_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = PeersArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        region: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::peers(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected connection-refused error"))?
        .to_string();
    assert!(
        err.contains("refused"),
        "error should mention 'refused', got: {err}"
    );
    Ok(())
}

/// Mirror the connection-refused / zero-timeout coverage on the
/// `decdn node health` path so the new CLI surface fails the same
/// way operators already expect for `node peers`.
#[tokio::test]
async fn cli_health_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = HealthArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::health(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected connection-refused error"))?
        .to_string();
    assert!(
        err.contains("refused"),
        "error should mention 'refused', got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_health_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = HealthArgs {
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::health(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected zero-timeout error"))?
        .to_string();
    assert!(
        err.contains("--timeout-ms"),
        "error should mention the flag, got: {err}"
    );
    Ok(())
}

/// `--timeout-ms 0` must be rejected up front — jsonrpsee interprets
/// `Duration::ZERO` as "never time out" rather than "sub-millisecond
/// deadline", which would hang an operator script that meant to cap
/// the wait. The guard in `commands::peers` fires before the client
/// is built, so any URL works here.
#[tokio::test]
async fn cli_peers_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = PeersArgs {
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        region: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::peers(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected zero-timeout error"))?
        .to_string();
    assert!(
        err.contains("--timeout-ms"),
        "error should mention the flag, got: {err}"
    );
    Ok(())
}

/// Mirror the connection-refused coverage onto `decdn node evict`. The
/// new CLI subcommand routes through the same `classify_client_error`
/// path as `peers` / `health`, but a regression that swallowed the
/// classification (or printed a misleading "evicted" line on failure)
/// would not be caught by the admin-RPC unit tests alone.
#[tokio::test]
async fn cli_evict_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = EvictArgs {
        hash: "0".repeat(64),
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::evict(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected connection-refused error"))?
        .to_string();
    assert!(
        err.contains("refused"),
        "error should mention 'refused', got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_evict_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = EvictArgs {
        hash: "0".repeat(64),
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::evict(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected zero-timeout error"))?
        .to_string();
    assert!(
        err.contains("--timeout-ms"),
        "error should mention the flag, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_announce_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = AnnounceArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::announce(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected connection-refused error"))?
        .to_string();
    assert!(
        err.contains("refused"),
        "error should mention 'refused', got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_announce_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = AnnounceArgs {
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::announce(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected zero-timeout error"))?
        .to_string();
    assert!(
        err.contains("--timeout-ms"),
        "error should mention the flag, got: {err}"
    );
    Ok(())
}

/// Same coverage on `decdn node reload`: surface the connection-refused
/// hint when the admin port is dead. The unit tests in `admin.rs` cover
/// the server-side branches; this guards the CLI's
/// `classify_client_error` path so a regression there can't reach
/// operators as a silent hang or unhelpful message.
#[tokio::test]
async fn cli_reload_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = ReloadArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::reload(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected connection-refused error"))?
        .to_string();
    assert!(
        err.contains("refused"),
        "error should mention 'refused', got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_reload_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = ReloadArgs {
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::reload(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected zero-timeout error"))?
        .to_string();
    assert!(
        err.contains("--timeout-ms"),
        "error should mention the flag, got: {err}"
    );
    Ok(())
}

/// `admin_v1_drain` over a live server fires the trigger and returns
/// `initiated: true`. The trigger itself wakes up the `wait()` call on the
/// other end — here we check just the wire-level response because wiring
/// the trigger into a real runtime is the runtime's integration test
/// territory (that would require starting `run()` in a background task,
/// which is test-infrastructure cost). The unit tests in `admin.rs`
/// prove the trigger fires; this test proves the RPC path reaches it.
#[tokio::test]
async fn admin_v1_drain_returns_initiated_true() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let (cache, _tmp) = test_cache().await?;
    let drain_trigger = Arc::new(DrainTrigger::new());
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::clone(&drain_trigger),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.drain().await?;
    assert!(resp.initiated, "expected initiated=true");

    // The RPC handler should have fired the trigger.
    let waited =
        tokio::time::timeout(std::time::Duration::from_millis(100), drain_trigger.wait()).await;
    assert!(waited.is_ok(), "drain RPC did not fire the DrainTrigger");

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// Mirror the connection-refused coverage onto `decdn node drain` so the
/// CLI path is guarded the same way as `peers`, `health`, `reload`, etc.
#[tokio::test]
async fn cli_drain_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = DrainArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::drain(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected connection-refused error"))?
        .to_string();
    assert!(
        err.contains("refused"),
        "error should mention 'refused', got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_drain_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = DrainArgs {
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::drain(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected zero-timeout error"))?
        .to_string();
    assert!(
        err.contains("--timeout-ms"),
        "error should mention the flag, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn admin_shutdown_closes_listener() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    // Baseline: server is up.
    let client = HttpClientBuilder::default().build(&url)?;
    client.peers_list().await?;

    let _ = stop_tx.send(());
    join.await?;

    // After shutdown the port should refuse new connections fairly
    // quickly. A fresh client is needed because jsonrpsee's HttpClient
    // uses a connection pool that might otherwise retry silently.
    // Bound the wait so a stuck test fails instead of hanging.
    let client2 = HttpClientBuilder::default().build(&url)?;
    let res = tokio::time::timeout(std::time::Duration::from_secs(2), client2.peers_list()).await;
    match res {
        Ok(Ok(_)) => panic!("admin server still responding after shutdown"),
        // Either a jsonrpsee error or the outer timeout are acceptable
        // — both mean "no longer serving".
        Ok(Err(_)) | Err(_) => Ok(()),
    }
}
