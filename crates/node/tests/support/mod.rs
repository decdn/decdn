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
use std::time::Duration;

use alloy::dyn_abi::Eip712Domain;
use alloy::signers::local::PrivateKeySigner;
use decdn_cache::{CacheEngine, FilesystemOrigin, Hash};
use decdn_common::config::ResolvedSecurity;
use decdn_incentive::PoolStateStore;
use decdn_node::dispatch::ConnectionLimiter;
use decdn_node::handlers::client::{ClientHandler, ClientHandlerDeps};
use decdn_node::metrics::Metrics;
use decdn_node::receipt_log::{DirectReceiptSink, DownloadReceipt, ReceiptLog, ReceiptSink};
use decdn_protocol::client::ClientMessage;
use decdn_protocol::{decode_message, encode_message, read_frame, write_frame};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, RelayMode, SecretKey, endpoint::presets};

/// Fresh random iroh identity.
pub fn fresh_key() -> SecretKey {
    SecretKey::generate()
}

/// The bao verified-stream WIRE size (content + interleaved proof, ADR 038) for a
/// `[byte_offset, byte_offset + byte_len)` request — the quantity the paid
/// `cdn/client/v1` delivery now meters, vouchers, and records. `byte_len == 0`
/// means "to end". The serve side aligns the request up to 16 KiB chunk groups
/// and emits the whole aligned superset, so the metered/paid amount is this, not
/// the requested content length. Callers that used `payload.len()` for a
/// bytes-delivered / receipt / region assertion now use this.
#[must_use]
pub fn bao_wire_len(total: u64, byte_offset: u64, byte_len: u64) -> u64 {
    match decdn_cache::range_pull::align_range(byte_offset, byte_len, total) {
        Ok(aligned) => decdn_cache::range_pull::bao_encoded_size(total, aligned.chunk_ranges()),
        // align_range only rejects an offset past the blob; tests pass valid
        // offsets, so the content size is a safe (never-hit) fallback.
        Err(_) => total,
    }
}

/// Whole-blob convenience for [`bao_wire_len`].
#[must_use]
pub fn bao_wire_len_whole(total: u64) -> u64 {
    bao_wire_len(total, 0, 0)
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

/// How long teardown waits for every endpoint to drain before it gives up.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Abort `tasks`, then close `endpoints` under one shared deadline.
///
/// The abort comes first because [`Endpoint::close`] waits for in-flight
/// connections to drain, and a handler task parked mid-stream holds one open.
/// Closing an endpoint whose accept loop still runs is what wedges teardown.
///
/// The deadline is shared across the whole call rather than applied per
/// endpoint, so a wedge costs [`SHUTDOWN_TIMEOUT`] once instead of once per
/// endpoint. The `drained/total` count in the warning names the position that
/// stalled, and nextest attributes the line to the test that printed it.
///
/// A timeout warns and returns instead of failing: teardown runs after every
/// assertion, so the test's real verdict is already decided by the time this
/// is reached. `.config/nextest.toml` is the hard backstop for a wedge that
/// outlives even this.
pub async fn shutdown<const N: usize, const M: usize>(
    tasks: [tokio::task::AbortHandle; N],
    endpoints: [&Endpoint; M],
) {
    for task in &tasks {
        task.abort();
    }
    let mut drained = 0usize;
    let closed = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
        for ep in endpoints {
            ep.close().await;
            drained += 1;
        }
    })
    .await;
    if closed.is_err() {
        eprintln!(
            "shutdown: endpoint close timed out after {SHUTDOWN_TIMEOUT:?} \
             ({drained}/{M} drained)"
        );
    }
}

/// EIP-712 domains a [`ClientHandler`] needs: slash-receipt, voucher, and
/// client-binding. Grouped so callers thread one value through the builders.
#[derive(Clone)]
pub struct HandlerDomains {
    pub slash: Eip712Domain,
    pub voucher: Eip712Domain,
    pub binding: Eip712Domain,
}

/// In-memory [`ReceiptLog`] fake for tests: collects appended receipts so a
/// suite can assert what was recorded on the voucher-accept path (issue #248).
#[derive(Debug, Default)]
pub struct VecReceiptLog {
    inner: std::sync::Mutex<Vec<DownloadReceipt>>,
}

impl VecReceiptLog {
    /// Snapshot the receipts appended so far.
    ///
    /// Recovers the inner `Vec` even if the mutex was poisoned by a panic on
    /// another thread (`PoisonError::into_inner`), so a failing assertion still
    /// sees the receipts actually collected rather than a misleading empty list.
    /// The anti-panic policy rules out `unwrap`/`expect` here, and silently
    /// defaulting to empty (the prior `unwrap_or_default`) would mask the real
    /// failure cause in a test double.
    #[must_use]
    pub fn snapshot(&self) -> Vec<DownloadReceipt> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ReceiptLog for VecReceiptLog {
    fn append(&self, receipt: &DownloadReceipt) -> std::io::Result<()> {
        self.inner
            .lock()
            .map_err(|_| std::io::Error::other("VecReceiptLog mutex poisoned"))?
            .push(receipt.clone());
        Ok(())
    }
}

/// A [`ReceiptLog`] whose `append` always errors. Proves the voucher-accept
/// path treats a receipt-log write failure as non-fatal (issue #248): the
/// payment already committed to the channel store, so delivery must still
/// succeed.
#[derive(Debug, Default)]
pub struct FailingReceiptLog;

impl ReceiptLog for FailingReceiptLog {
    fn append(&self, _receipt: &DownloadReceipt) -> std::io::Result<()> {
        Err(std::io::Error::other("simulated receipt-log write failure"))
    }
}

/// A [`ReceiptLog`] whose `append` blocks until [`release`](Self::release) is
/// called, recording what it eventually appends. Lets a test prove the
/// paid-delivery hot path never waits on receipt-log I/O (#803): wired behind
/// the real background-writer sink, delivery must complete even while every
/// `append` is stalled here (the buggy pre-#803 code awaited the append inline).
#[derive(Default)]
pub struct BlockingReceiptLog {
    state: std::sync::Mutex<BlockingReceiptState>,
    released: std::sync::Condvar,
}

#[derive(Default)]
struct BlockingReceiptState {
    released: bool,
    seen: Vec<DownloadReceipt>,
}

impl BlockingReceiptLog {
    /// Unblock all current and future `append` calls.
    pub fn release(&self) {
        if let Ok(mut g) = self.state.lock() {
            g.released = true;
        }
        self.released.notify_all();
    }

    /// Snapshot the receipts appended so far.
    #[must_use]
    pub fn snapshot(&self) -> Vec<DownloadReceipt> {
        self.state
            .lock()
            .map(|g| g.seen.clone())
            .unwrap_or_default()
    }
}

impl ReceiptLog for BlockingReceiptLog {
    fn append(&self, receipt: &DownloadReceipt) -> std::io::Result<()> {
        let mut g = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("BlockingReceiptLog mutex poisoned"))?;
        while !g.released {
            g = self
                .released
                .wait(g)
                .map_err(|_| std::io::Error::other("BlockingReceiptLog mutex poisoned"))?;
        }
        g.seen.push(receipt.clone());
        Ok(())
    }
}

/// Build a [`ClientHandler`] over `cache`/`store` with explicit domains and the
/// `max_blob_size_bytes` (`0` == unlimited) / `max_concurrent_streams` knobs.
/// Uses a throwaway in-memory receipt log; tests asserting receipt contents use
/// [`build_handler_full_with_receipts`].
#[allow(clippy::too_many_arguments)]
pub fn build_handler_full(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    rate: u64,
    domains: &HandlerDomains,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
) -> anyhow::Result<Arc<ClientHandler>> {
    build_handler_full_with_receipts(
        server_id,
        server_eth,
        metrics,
        limiter,
        cache,
        store,
        Arc::new(VecReceiptLog::default()),
        rate,
        domains,
        max_blob_size_bytes,
        max_concurrent_streams,
    )
}

/// Like [`build_handler_full`] but takes an explicit [`ReceiptLog`] so a test
/// can hold a handle ([`VecReceiptLog`]) and assert the appended receipts.
#[allow(clippy::too_many_arguments)]
pub fn build_handler_full_with_receipts(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    receipt_log: Arc<dyn ReceiptLog>,
    rate: u64,
    domains: &HandlerDomains,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
) -> anyhow::Result<Arc<ClientHandler>> {
    // The handler enqueues through a `ReceiptSink`; wrap the test's synchronous
    // `ReceiptLog` fake so receipts are appended inline and the test can assert
    // them deterministically without standing up the background writer.
    build_handler_full_with_sink(
        server_id,
        server_eth,
        metrics,
        limiter,
        cache,
        store,
        Arc::new(DirectReceiptSink::new(receipt_log)),
        rate,
        domains,
        max_blob_size_bytes,
        max_concurrent_streams,
    )
}

/// Like [`build_handler_full_with_receipts`] but wires an explicit
/// [`ReceiptSink`] — e.g. the real background-writer sink from
/// [`decdn_node::receipt_log::spawn_receipt_writer`] — so a test can exercise
/// the production enqueue path instead of the synchronous [`DirectReceiptSink`].
#[allow(clippy::too_many_arguments)]
pub fn build_handler_full_with_sink(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    receipt_sink: Arc<dyn ReceiptSink>,
    rate: u64,
    domains: &HandlerDomains,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
) -> anyhow::Result<Arc<ClientHandler>> {
    Ok(Arc::new(ClientHandler::new(client_handler_deps(
        server_id,
        server_eth,
        metrics,
        limiter,
        cache,
        store,
        receipt_sink,
        rate,
        domains,
        max_blob_size_bytes,
        max_concurrent_streams,
    ))?))
}

/// The required-deps [`ClientHandlerDeps`] shared by the builders — every
/// optional wiring hook left `None`. Callers set the optionals they need.
#[allow(clippy::too_many_arguments)]
fn client_handler_deps(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    receipt_sink: Arc<dyn ReceiptSink>,
    rate: u64,
    domains: &HandlerDomains,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
) -> ClientHandlerDeps {
    ClientHandlerDeps::new(
        server_id,
        Arc::clone(metrics),
        limiter,
        cache,
        Arc::clone(server_eth),
        domains.slash.clone(),
        domains.voucher.clone(),
        domains.binding.clone(),
        store,
        receipt_sink,
        rate,
        decdn_node::rate_bounds::RateBounds::new(0),
        max_blob_size_bytes,
        max_concurrent_streams,
        // Empty by default; a test needing a populated deny-set overwrites the
        // `content_deny` field via the `configure` closure of `build_handler_with`.
        Arc::new(decdn_node::content_deny::ContentDenylist::empty()),
        // No refundable-floor gate in the loopback fixtures: a seeded lane serves
        // from the first voucher regardless of the pool's remaining deposit.
        alloy::primitives::U256::ZERO,
        // Never sheds: the loopback fixtures exercise the paid-delivery path
        // itself, not overload protection.
        decdn_node::load_shed::LoadShedController::from_config(
            &decdn_common::config::ResolvedLoadShed {
                policy: decdn_common::config::LoadShedPolicyKind::AlwaysAdmit,
                ..Default::default()
            },
        ),
    )
}

/// Like [`build_handler_full`] but hands the assembled [`ClientHandlerDeps`] to
/// `configure` before construction — the construction-time replacement for the
/// removed `attach_*` setters (#1254). A test enables the optional wiring it
/// exercises (pull-through deadline, window origin, leech governor, idle timeout,
/// …) by setting the matching `deps` fields. Uses the throwaway in-memory receipt
/// log, like [`build_handler_full`].
#[allow(clippy::too_many_arguments)]
pub fn build_handler_full_configured(
    server_id: iroh::PublicKey,
    server_eth: &Arc<PrivateKeySigner>,
    metrics: &Arc<Metrics>,
    limiter: Arc<ConnectionLimiter>,
    cache: CacheEngine,
    store: Arc<dyn PoolStateStore>,
    rate: u64,
    domains: &HandlerDomains,
    max_blob_size_bytes: u64,
    max_concurrent_streams: usize,
    configure: impl FnOnce(&mut ClientHandlerDeps),
) -> anyhow::Result<Arc<ClientHandler>> {
    let mut deps = client_handler_deps(
        server_id,
        server_eth,
        metrics,
        limiter,
        cache,
        store,
        Arc::new(DirectReceiptSink::new(Arc::new(VecReceiptLog::default()))),
        rate,
        domains,
        max_blob_size_bytes,
        max_concurrent_streams,
    );
    configure(&mut deps);
    Ok(Arc::new(ClientHandler::new(deps)?))
}

/// Read one length-framed [`ClientMessage`] from `recv`. Mirrors the requester's
/// private `client_requester::read_client_message`, exposed for the raw fake
/// clients/servers the `cdn/client/v1` integration binaries hand-roll.
pub async fn read_client_msg(recv: &mut RecvStream) -> anyhow::Result<ClientMessage> {
    let frame = read_frame(recv)
        .await
        .map_err(|e| anyhow::anyhow!("read frame: {e}"))?;
    let (msg, _rest) =
        decode_message::<ClientMessage>(&frame).map_err(|e| anyhow::anyhow!("decode: {e}"))?;
    Ok(msg)
}

/// Write one length-framed [`ClientMessage`] to `send` (the write-side twin of
/// [`read_client_msg`]).
pub async fn write_client_msg(send: &mut SendStream, msg: &ClientMessage) -> anyhow::Result<()> {
    let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    write_frame(send, &payload)
        .await
        .map_err(|e| anyhow::anyhow!("write: {e}"))
}

/// Accept exactly one inbound connection on `ep` — the [`spawn_server`] accept
/// plumbing (`incoming.accept()` then `connecting.await`) factored out for the
/// one-shot raw servers that don't run a full accept loop.
pub async fn accept_one(ep: &Endpoint) -> anyhow::Result<Connection> {
    let incoming = ep
        .accept()
        .await
        .ok_or_else(|| anyhow::anyhow!("endpoint closed before a connection arrived"))?;
    let connecting = incoming
        .accept()
        .map_err(|e| anyhow::anyhow!("incoming accept: {e}"))?;
    connecting
        .await
        .map_err(|e| anyhow::anyhow!("connecting await: {e}"))
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
