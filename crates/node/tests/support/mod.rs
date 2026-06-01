//! Shared helpers for `cdn/client/v1` paid-delivery integration tests.
//!
//! These primitives spin up real in-process iroh endpoints, caches, and
//! [`ClientHandler`]s on localhost. They are intentionally domain-agnostic: the
//! EIP-712 domains (slash / voucher / binding) are passed in by the caller so
//! both the fixed-constant loopback suite (`client_loopback.rs`) and the live
//! on-chain settlement e2e (`anvil_settlement_e2e.rs`, issue #745, which must
//! use the deployed contract addresses + anvil chain id) can share them.
//!
//! Included via `mod support;` by each integration-test binary that needs it;
//! files under `tests/` subdirectories are not compiled as their own test
//! binaries. Not every binary uses every helper, hence the crate-level
//! `dead_code` allow.

#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use alloy::dyn_abi::Eip712Domain;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, FilesystemOrigin, Hash};
use decdn_common::config::ResolvedSecurity;
use decdn_incentive::ChannelStateStore;
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::client::ClientHandler;
use decdn_node::metrics::Metrics;
use decdn_protocol::MAX_RATE_PER_MB;
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets};

/// Fresh random iroh identity.
pub fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

/// Open an empty cache (no origins) in a fresh temp dir.
pub async fn empty_cache() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), vec![], 16).await?;
    Ok((cache, tmp))
}

/// Open a cache pre-seeded with `payload` (pulled+verified via a filesystem
/// origin, then the origin dir is dropped). Returns the cache, blob hash, and
/// the cache temp dir to keep alive.
pub async fn cache_with_blob(
    payload: &[u8],
) -> anyhow::Result<(CacheEngine, Hash, tempfile::TempDir)> {
    let hash = Hash::new(payload);
    let origin_dir = tempfile::tempdir()?;
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let dir = origin_dir.path().join(shard);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(hex.as_str()), payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let cache = CacheEngine::open(
        cache_dir.path(),
        vec![origin as Arc<dyn decdn_cache::Origin>],
        16,
    )
    .await?;
    let _ = cache.get(hash).await?; // populate local store
    drop(origin_dir);
    Ok((cache, hash, cache_dir))
}

/// A connection limiter with all gates wide open (the common-case test setup).
pub fn permissive_limiter(metrics: &Arc<Metrics>) -> Arc<ConnectionLimiter> {
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1_000_000.0,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    };
    Arc::new(ConnectionLimiter::new(&cfg, Arc::clone(metrics)))
}

/// Bind a loopback iroh endpoint with relays disabled, returning the endpoint
/// and a dialable IPv4 socket address (loopback-rewritten if bound to 0.0.0.0).
pub async fn local_endpoint(
    secret_key: SecretKey,
    alpns: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, SocketAddr)> {
    let bind = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(bind)
        .map_err(|e| anyhow::anyhow!("bind_addr: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("bind: {e}"))?;
    let addr = ep
        .bound_sockets()
        .into_iter()
        .find(SocketAddr::is_ipv4)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
    let addr = match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, v4.port()))
        }
        other => other,
    };
    Ok((ep, addr))
}

/// EIP-712 domains a [`ClientHandler`] needs: slash-receipt, voucher, and
/// client-binding. Grouped so callers thread one value through the builders.
#[derive(Clone)]
pub struct HandlerDomains {
    pub slash: Eip712Domain,
    pub voucher: Eip712Domain,
    pub binding: Eip712Domain,
}

/// Build a [`ClientHandler`] over `cache`/`store` with explicit domains and the
/// `max_blob_size_bytes` (`0` == unlimited) / `max_concurrent_streams` knobs.
#[allow(clippy::too_many_arguments)]
pub fn build_handler_full(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn ChannelStateStore>,
    rate: u64,
    domains: &HandlerDomains,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
) -> anyhow::Result<Arc<ClientHandler>> {
    Ok(Arc::new(ClientHandler::new(
        server_id,
        Arc::clone(metrics),
        limiter,
        cache,
        Arc::clone(server_eth),
        domains.slash.clone(),
        domains.voucher.clone(),
        domains.binding.clone(),
        store,
        Arc::new(AtomicU64::new(rate)),
        0,
        MAX_RATE_PER_MB,
        1, // voucher_interval_mb
        max_blob_size_bytes,
        max_concurrent_streams,
    )?))
}

/// Spawn a server endpoint running `handler`, accepting connections until the
/// endpoint closes.
pub fn spawn_server(
    server_ep: Endpoint,
    handler: Arc<ClientHandler>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = server_ep.accept().await {
            let Ok(connecting) = incoming.accept() else {
                continue;
            };
            let Ok(conn) = connecting.await else { continue };
            let _ = handler.accept(conn).await;
        }
    })
}
