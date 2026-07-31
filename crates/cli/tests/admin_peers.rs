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
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

/// Test-only helper so call sites stay compact. The `unwrap` is
/// statically sound for any positive integer literal we pass in.
const fn nz(v: u64) -> NonZeroU64 {
    match NonZeroU64::new(v) {
        Some(n) => n,
        None => panic!("test value must be > 0"),
    }
}

use decdn_cache::CacheEngine;
use decdn_cli::commands::node as commands;
use decdn_common::admin::{AdminRpcClient, DrainRequest};
use decdn_common::cli::{
    AnnounceArgs, ChannelsArgs, DrainArgs, EvictArgs, HealthArgs, PeersArgs, ReloadArgs, StatusArgs,
};
use decdn_gossip::PeerTable;
use decdn_incentive::{ChannelState, ChannelStateStore, MemoryChannelStateStore, VoucherActivity};
use decdn_node::admin::{self, AdminState, ChannelStatusHandles, DhtStatusHandles, DrainTrigger};
use decdn_node::dht::routing::NodeId;
use decdn_node::dht::{
    ConfigStakerSet, RecordStore, RecordStoreConfig, RepublishScheduler, StakerSet,
};
use decdn_node::metrics::Metrics;
use decdn_protocol::{ContentHash, NodeAnnounce, NodeAnnounceBody};
use jsonrpsee::RpcModule;
use jsonrpsee::core::ClientError;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use jsonrpsee::server::{Server, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, oneshot};

fn mk_announce(node_id: [u8; 32], region: &str, ts_us: u64) -> NodeAnnounce {
    NodeAnnounce {
        body: NodeAnnounceBody {
            node_id,
            region: region.to_string(),
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
    let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
    Ok((cache, tmp))
}

/// Throwaway `PrivateKeySigner` for `AdminState::new` callers that don't
/// exercise signing logic. Chain id sourced from the canonical const so it
/// stays in lock-step with the runtime loader.
fn throwaway_signer() -> Arc<alloy::signers::local::PrivateKeySigner> {
    use alloy::signers::Signer;
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::eth_identity::ARBITRUM_SEPOLIA_CHAIN_ID;
    Arc::new(PrivateKeySigner::random().with_chain_id(Some(ARBITRUM_SEPOLIA_CHAIN_ID)))
}

/// Build `DhtStatusHandles` seeded with two peers in distinct buckets
/// (0 and 255), two stakers, one provider record, one scheduled republish,
/// and a fixed refresh clock — enough for an end-to-end `admin_v1_status`
/// round-trip over real HTTP. Mirrors the unit-level seed in `admin.rs`.
fn seeded_dht_handles() -> DhtStatusHandles {
    let mut table = decdn_node::dht::RoutingTable::new(NodeId::from_bytes([0u8; 32]));
    let mut high = [0u8; 32];
    high[0] = 0x80; // bucket 255
    let mut low = [0u8; 32];
    low[31] = 0x01; // bucket 0
    table.insert(NodeId::from_bytes(high));
    table.insert(NodeId::from_bytes(low));

    let mut stakers = std::collections::HashSet::new();
    stakers.insert(NodeId::from_bytes([1u8; 32]));
    stakers.insert(NodeId::from_bytes([2u8; 32]));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(stakers));

    let mut store = RecordStore::new(RecordStoreConfig::default());
    store.insert_at(
        NodeId::from_bytes([5u8; 32]),
        ContentHash::from_bytes([7u8; 32]),
        1_000,
    );

    let republish = Arc::new(RepublishScheduler::new());
    republish.schedule_steady(ContentHash::from_bytes([9u8; 32]));

    DhtStatusHandles {
        routing: Arc::new(std::sync::Mutex::new(table)),
        staker_set,
        record_store: Arc::new(std::sync::Mutex::new(store)),
        republish,
        refresh_clock: Arc::new(std::sync::atomic::AtomicU64::new(1_700_000_000_000_000)),
        refresh_interval: std::time::Duration::from_hours(1),
    }
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
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
        throwaway_signer(),
        Arc::new(Metrics::new()),
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
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
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
        throwaway_signer(),
        Arc::new(Metrics::new()),
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
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
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
        throwaway_signer(),
        Arc::new(Metrics::new()),
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
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
        throwaway_signer(),
        Arc::new(Metrics::new()),
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

/// `admin_v1_channels` round-trips through real HTTP via jsonrpsee's
/// generated client (issue #749): a store seeded with two channels
/// surfaces both, ordered by descending outstanding (no activity
/// recorded), with the configured threshold echoed and eligibility
/// computed against it.
#[tokio::test]
async fn channels_round_trips_seeded_store() -> anyhow::Result<()> {
    use alloy::primitives::{Address, U256};

    let token: Address = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".parse()?;
    // `signer_byte` distinguishes the pinned voucher signer from the funder so
    // the admin surface is proven to report both, not the funder twice.
    let mk =
        |id_byte: u8, signer_byte: u8, amount: u64, deposit: u64, nonce: u64| -> ChannelState {
            let mut id = [0u8; 32];
            id[31] = id_byte;
            let mut client = [0u8; 20];
            client[19] = id_byte;
            let mut signer = [0u8; 20];
            signer[19] = signer_byte;
            ChannelState::hydrate(
                id.into(),
                Address::from(client),
                Address::from(signer),
                token,
                U256::from(deposit),
                U256::from(amount),
                U256::from(nonce),
                U256::from(amount),
                None,
                0,
                false,
            )
        };

    let store = Arc::new(MemoryChannelStateStore::new());
    // Channel 1 delegates signing to a distinct key; channel 2 self-signs.
    store.record(&mk(1, 0xAA, 2_000_000, 10_000_000, 5))?;
    store.record(&mk(2, 2, 100_000, 5_000_000, 2))?;

    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
        throwaway_signer(),
        Arc::new(Metrics::new()),
    )
    .with_channels(ChannelStatusHandles {
        channel_store: store as Arc<dyn ChannelStateStore>,
        voucher_activity: Arc::new(VoucherActivity::new()),
        redeem_threshold_micro_usdc: 1_000_000,
    });
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.channels().await?;
    assert_eq!(resp.redeem_threshold_micro_usdc, 1_000_000);
    assert_eq!(resp.channels.len(), 2);
    // No activity → ordered by descending outstanding.
    let first = resp
        .channels
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing first channel"))?;
    assert_eq!(first.outstanding_micro_usdc, 2_000_000);
    assert_eq!(first.last_nonce, 5);
    assert!(first.settlement_eligible, "2 USDC >= 1 USDC threshold");
    assert!(first.channel_id.starts_with("0x"));
    assert!(first.counterparty.starts_with("0x"));
    assert_eq!(
        first.voucher_signer,
        Address::from([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xAA
        ])
        .to_string(),
        "the delegated voucher signer must reach the admin surface"
    );
    assert_ne!(
        first.voucher_signer, first.counterparty,
        "funder and signer must not collapse onto one address"
    );
    assert_eq!(first.seconds_since_last_voucher, None);
    let second = resp
        .channels
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing second channel"))?;
    assert_eq!(second.outstanding_micro_usdc, 100_000);
    assert!(!second.settlement_eligible, "0.1 USDC < 1 USDC threshold");
    assert_eq!(
        second.voucher_signer, second.counterparty,
        "a self-signing channel reports the funder as its signer"
    );

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// `admin_v1_channels` on a node with no channel handles wired returns
/// an empty list and a zero threshold over the wire — exercising the
/// `with_channels`-absent path end-to-end.
#[tokio::test]
async fn channels_without_handles_returns_empty_over_http() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
        throwaway_signer(),
        Arc::new(Metrics::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.channels().await?;
    assert!(resp.channels.is_empty());
    assert_eq!(resp.redeem_threshold_micro_usdc, 0);

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// CLI `channels` against a dropped listener surfaces the
/// connection-refused hint, same as the peers path.
#[tokio::test]
async fn cli_channels_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = ChannelsArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::channels(&args, None)
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
        dry_run: false,
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
        dry_run: false,
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

/// #845: a server that reports `triggered=false` (the announce was accepted
/// but not queued) must surface as a non-zero exit, not a silent `Ok(())` with
/// `announce_queued=false` on stdout. Well-formed nodes never return this shape
/// (publisher-disabled is a distinct error code), so it is treated as failure.
#[tokio::test]
async fn cli_announce_reports_untriggered_as_error() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    let std_listener = listener.into_std()?;
    let config = ServerConfig::builder().http_only().build();
    let server = Server::builder()
        .set_config(config)
        .build_from_tcp(std_listener)
        .map_err(|e| anyhow::anyhow!("build fake admin: {e}"))?;
    let mut module = RpcModule::new(());
    module.register_async_method("admin_v1_announce", |_p, _c, _e| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "triggered": false,
        }))
    })?;
    let handle = server.start(module);

    let args = AnnounceArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 5_000,
    };
    let err = commands::announce(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected untriggered-announce error"))?
        .to_string();
    assert!(
        err.contains("not queued") && err.contains("triggered=false"),
        "error should explain the announce was not queued, got: {err}"
    );

    handle.stop().ok();
    handle.stopped().await;
    Ok(())
}

/// Counterpart to the above: `triggered=true` is the normal acceptance and
/// must return `Ok(())` (exit 0).
#[tokio::test]
async fn cli_announce_triggered_is_ok() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    let std_listener = listener.into_std()?;
    let config = ServerConfig::builder().http_only().build();
    let server = Server::builder()
        .set_config(config)
        .build_from_tcp(std_listener)
        .map_err(|e| anyhow::anyhow!("build fake admin: {e}"))?;
    let mut module = RpcModule::new(());
    module.register_async_method("admin_v1_announce", |_p, _c, _e| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "triggered": true,
        }))
    })?;
    let handle = server.start(module);

    let args = AnnounceArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 5_000,
    };
    commands::announce(&args, None)
        .await
        .map_err(|e| anyhow::anyhow!("triggered=true should succeed, got: {e}"))?;

    handle.stop().ok();
    handle.stopped().await;
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
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
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
        throwaway_signer(),
        Arc::new(Metrics::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.drain(Some(DrainRequest::default())).await?;
    assert!(resp.initiated, "expected initiated=true");
    assert!(
        !resp.wait_admin_honored,
        "default DrainRequest must report wait_admin_honored=false"
    );
    // Default request leaves wait_admin false → SIGTERM-equivalent
    // ordering preserved.
    assert!(
        !drain_trigger.wait_admin(),
        "default DrainRequest must not flip wait_admin on"
    );

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
        wait: false,
        wait_timeout_secs: nz(30),
        wait_poll_ms: nz(250),
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
        wait: false,
        wait_timeout_secs: nz(30),
        wait_poll_ms: nz(250),
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

/// A caller (`curl`, a Python script) that invokes `admin_v1_drain`
/// with no `params` field at all must still trigger drain and return
/// the default response. Without `req: Option<DrainRequest>` on the
/// trait, jsonrpsee's proc-macro uses `next()` and would return
/// JSON-RPC `-32602 Invalid params`. We bypass the generated client
/// (which always sends `Some(...)`) and call the raw `request` method
/// with `rpc_params![]` to reproduce the parameter-less wire shape.
#[tokio::test]
async fn admin_v1_drain_with_empty_params_still_triggers() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
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
        throwaway_signer(),
        Arc::new(Metrics::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    // Raw call: `rpc_params![]` serializes as `"params":[]`, which
    // is how a pre-#604 client invokes a parameter-less method.
    let resp: decdn_common::admin::DrainResponse =
        client.request("admin_v1_drain", rpc_params![]).await?;
    assert!(resp.initiated, "expected initiated=true on no-params drain");
    assert!(
        !resp.wait_admin_honored,
        "no-params drain must report wait_admin_honored=false"
    );
    // Server-side observable effect: trigger fired and wait_admin
    // stayed false.
    assert!(
        !drain_trigger.wait_admin(),
        "no-params drain must keep wait_admin=false"
    );
    let waited =
        tokio::time::timeout(std::time::Duration::from_millis(100), drain_trigger.wait()).await;
    assert!(
        waited.is_ok(),
        "no-params drain must still fire the trigger"
    );

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// `decdn node drain --wait` succeeds when `in_flight_streams` is
/// already 0 — the server's gauge starts at 0 and the polling loop
/// returns on the first `health()` tick.
#[tokio::test]
async fn cli_drain_wait_returns_immediately_when_idle() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
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
        throwaway_signer(),
        Arc::new(Metrics::new()),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let args = DrainArgs {
        admin_url: Some(url.clone()),
        config: None,
        json: false,
        timeout_ms: 5_000,
        wait: true,
        wait_timeout_secs: nz(10),
        wait_poll_ms: nz(50),
    };
    commands::drain(&args, None).await?;

    // Server-side: drain RPC fired with wait_admin=true.
    assert!(
        drain_trigger.wait_admin(),
        "wait flag should be set on the server's trigger"
    );

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// `decdn node drain --wait` fails fast (no polling) when the server
/// responds with `wait_admin_honored: false` — a server that can't keep
/// admin alive through the drain, or a future regression where the
/// runtime ordering wasn't reapplied. Without this guard, the imminent
/// ECONNREFUSED from the early admin tear-down would be misread as drain
/// completion while in-flight streams keep running — exactly the
/// false-success the reviewer flagged.
///
/// The simulated server is a minimal jsonrpsee `RpcModule` that hand-
/// rolls an `admin_v1_drain` returning `wait_admin_honored: false`. No
/// `admin_v1_health` is registered — the CLI must reject before ever
/// polling, so the absence proves the guard fired.
#[tokio::test]
async fn cli_drain_wait_refuses_unhonored_server() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    let std_listener = listener.into_std()?;
    let config = ServerConfig::builder().http_only().build();
    let server = Server::builder()
        .set_config(config)
        .build_from_tcp(std_listener)
        .map_err(|e| anyhow::anyhow!("build fake admin: {e}"))?;
    let mut module = RpcModule::new(());
    module.register_async_method("admin_v1_drain", |_params, _ctx, _ext| async move {
        // Server reports it will not keep admin alive through the drain,
        // triggering the CLI's safety guard.
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "initiated": true,
            "wait_admin_honored": false,
        }))
    })?;
    let handle = server.start(module);

    let url = format!("http://{addr}");
    let args = DrainArgs {
        admin_url: Some(url.clone()),
        config: None,
        json: false,
        timeout_ms: 5_000,
        wait: true,
        wait_timeout_secs: nz(30),
        wait_poll_ms: nz(250),
    };
    let err = commands::drain(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected unhonored-server error"))?
        .to_string();
    assert!(
        err.contains("wait_admin_honored=false"),
        "error should mention wait_admin_honored=false, got: {err}"
    );

    handle.stop().ok();
    handle.stopped().await;
    Ok(())
}

/// `decdn node drain --wait` against a server that *does* honor
/// `wait_admin` but then closes admin mid-poll (the real
/// late-stop success terminal): the polling client's `health()`
/// call returns `ECONNREFUSED`, which the CLI treats as drain
/// completion. Without this test the ECONNREFUSED branch of the
/// polling loop is uncovered — a regression in
/// `is_admin_closed_transport_error`'s source-chain walk or in the
/// match-arm ordering would silently turn drain success into `Err`.
#[tokio::test]
async fn cli_drain_wait_treats_econnrefused_as_complete() -> anyhow::Result<()> {
    // Build a fake server that:
    //   1. Answers admin_v1_drain with wait_admin_honored=true.
    //   2. Stops the server as soon as the first admin_v1_health
    //      call lands, so the polling loop's *next* tick gets
    //      ECONNREFUSED — exactly the late-stop sequence the
    //      runtime executes in production.
    use std::sync::OnceLock;

    let (listener, addr) = bind_loopback().await?;
    let std_listener = listener.into_std()?;
    let config = ServerConfig::builder().http_only().build();
    let server = Server::builder()
        .set_config(config)
        .build_from_tcp(std_listener)
        .map_err(|e| anyhow::anyhow!("build fake admin: {e}"))?;
    let mut module = RpcModule::new(());
    module.register_async_method("admin_v1_drain", |_p, _c, _e| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "initiated": true,
            "wait_admin_honored": true,
        }))
    })?;
    // Set lazily: `Server::start()` is called *after* the module is
    // built, so the closure can't capture the handle directly.
    let handle_holder: Arc<OnceLock<jsonrpsee::server::ServerHandle>> = Arc::new(OnceLock::new());
    let handle_for_health = Arc::clone(&handle_holder);
    let stop_guard: Arc<OnceLock<()>> = Arc::new(OnceLock::new());
    module.register_async_method("admin_v1_health", move |_p, _c, _e| {
        let h = Arc::clone(&handle_for_health);
        let g = Arc::clone(&stop_guard);
        async move {
            // First health() call schedules a stop; subsequent
            // calls (if any race in before the stop lands) no-op
            // via the OnceLock guard.
            if g.set(()).is_ok()
                && let Some(handle) = h.get().cloned()
            {
                tokio::spawn(async move {
                    // Brief sleep so *this* poll's response is fully
                    // serialized before the listener closes — the
                    // CLI must observe `in_flight_streams=1` first,
                    // then the *next* poll hits ECONNREFUSED.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    handle.stop().ok();
                });
            }
            Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "node_id": "00".repeat(32),
                "uptime_s": 1,
                "in_flight_streams": 1,
            }))
        }
    })?;
    let handle = server.start(module);
    let _ = handle_holder.set(handle.clone());

    let url = format!("http://{addr}");
    let args = DrainArgs {
        admin_url: Some(url.clone()),
        config: None,
        json: false,
        timeout_ms: 5_000,
        wait: true,
        wait_timeout_secs: nz(10),
        // 25ms poll < 50ms server-side stop delay: the first poll
        // completes, the server then closes during the next sleep,
        // and the second poll fires ECONNREFUSED.
        wait_poll_ms: nz(25),
    };
    commands::drain(&args, None).await?;

    handle.stopped().await;
    Ok(())
}

/// `decdn node drain --wait` exits non-zero with a `drain_timeout=true`
/// diagnostic on stderr when the in-flight count stays above zero past
/// the wait budget. Simulates the stuck-stream case by holding a
/// dispatch permit for the duration of the test.
#[tokio::test]
async fn cli_drain_wait_times_out_on_stuck_stream() -> anyhow::Result<()> {
    use decdn_common::config::ResolvedSecurity;

    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let drain_trigger = Arc::new(DrainTrigger::new());
    let metrics = Arc::new(Metrics::new());
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::clone(&drain_trigger),
        throwaway_signer(),
        Arc::clone(&metrics),
    );
    let (url, stop_tx, join) = spawn_admin(state).await?;

    // Hold one permit for the lifetime of the test — the limiter shares
    // the same metrics handle, so `dispatch_in_flight` reads 1 from the
    // CLI's `health()` polls.
    let limiter = decdn_node::dispatch::ConnectionLimiter::new(
        &ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 1e9,
            per_source_burst: u32::MAX,
            max_tracked_sources: 16,
        },
        Arc::clone(&metrics),
    );
    let _held = limiter
        .acquire_for_test(Some(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST)))
        .ok()
        .ok_or_else(|| anyhow::anyhow!("permit acquire"))?;

    let args = DrainArgs {
        admin_url: Some(url.clone()),
        config: None,
        json: false,
        timeout_ms: 5_000,
        wait: true,
        // Sub-second wait budget so the test completes promptly.
        wait_timeout_secs: nz(1),
        wait_poll_ms: nz(50),
    };
    let err = commands::drain(&args, None)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected timeout error"))?
        .to_string();
    assert!(
        err.contains("timed out"),
        "error should mention timeout, got: {err}"
    );
    assert!(
        err.contains("1 stream(s) still in flight") || err.contains("1 stream(s)"),
        "error should mention 1 in-flight, got: {err}"
    );

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

#[tokio::test]
async fn admin_shutdown_closes_listener() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0u8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
        throwaway_signer(),
        Arc::new(Metrics::new()),
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

/// `admin_v1_status` round-trips through real HTTP: the seeded DHT health
/// (routing fills, staker count, record-store utilization, republish
/// depth, last-refresh timestamp) must survive serde + the generated
/// client bindings unchanged (issue #741).
#[tokio::test]
async fn status_round_trips_dht_health() -> anyhow::Result<()> {
    let peer_table = Arc::new(RwLock::new(PeerTable::new(0, 0)));
    let (cache, _tmp) = test_cache().await?;
    let state = AdminState::new(
        peer_table,
        [0xABu8; 32],
        Instant::now(),
        cache,
        None,
        None,
        Arc::new(DrainTrigger::new()),
        throwaway_signer(),
        Arc::new(Metrics::new()),
    )
    .with_dht(seeded_dht_handles());
    let (url, stop_tx, join) = spawn_admin(state).await?;

    let client = HttpClientBuilder::default().build(&url)?;
    let resp = client.status().await?;

    assert_eq!(resp.node_id, "ab".repeat(32));
    assert_eq!(resp.routing.total_peers, 2);
    assert_eq!(resp.routing.non_empty_buckets, 2);
    let indices: Vec<u16> = resp.routing.buckets.iter().map(|b| b.index).collect();
    assert_eq!(indices, vec![0, 255]);
    assert_eq!(resp.routing.bucket_capacity, 20);
    assert_eq!(resp.routing.refresh_interval_s, 3_600);
    assert_eq!(resp.routing.last_refresh_us, Some(1_700_000_000_000_000));
    assert_eq!(resp.known_stakers, 2);
    assert_eq!(resp.record_store.records, 1);
    assert_eq!(resp.record_store.capacity, 100_000);
    assert_eq!(resp.republish.scheduled_records, 1);

    let _ = stop_tx.send(());
    join.await?;
    Ok(())
}

/// `decdn node status` against a dead port surfaces the friendly
/// connection-refused message (not a panic), mirroring the peers path.
#[tokio::test]
async fn cli_status_surfaces_connection_refused() -> anyhow::Result<()> {
    let (listener, addr) = bind_loopback().await?;
    drop(listener);

    let args = StatusArgs {
        admin_url: Some(format!("http://{addr}")),
        config: None,
        json: false,
        timeout_ms: 2_000,
    };
    let err = commands::status(&args, None)
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

/// `decdn node status --timeout-ms 0` is rejected before the client is
/// built, same guard as the other admin subcommands.
#[tokio::test]
async fn cli_status_rejects_zero_timeout() -> anyhow::Result<()> {
    let args = StatusArgs {
        admin_url: Some("http://127.0.0.1:1".to_string()),
        config: None,
        json: false,
        timeout_ms: 0,
    };
    let err = commands::status(&args, None)
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
