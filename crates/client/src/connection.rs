//! Dialling `cdn/client/v1` connections, and a caller-owned connection kept warm
//! across many hashes.
//!
//! `dial` is the one dial every requester path uses, one-shot or warm. It runs
//! the dial on a caller-chosen runtime when the caller asks for one, so the
//! connection's QUIC driver lives there.
//!
//! [`WarmConnection`] dials once and stays open, so a caller fetching many hashes
//! from one provider pays the dial + NAT-traversal cost a single time and opens a
//! fresh bi-stream per hash instead. The wire is unchanged — one stream still
//! carries exactly one hash (`open_progressive_pull_on` opens the
//! bi-stream); only the client-side teardown differs, because a per-hash pull
//! leaves the connection open for the next one rather than closing it.
//!
//! [`Connections`] keeps one [`WarmConnection`] per node for a whole command,
//! so every lane and every entry of that command dials each node once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tokio::runtime::Handle;
use tracing::Instrument as _;

use decdn_protocol::ALPN_CLIENT;

use crate::{LocalPullFault, PullTimeout, rate_limited};

/// Dial `target` on the CDN client ALPN.
///
/// The dial spawns the connection's QUIC driver on whichever runtime runs the
/// `connect`, and the connection makes progress — I/O, timers, its close — only
/// while that driver runs. With `runtime` set, the dial runs on that runtime, so
/// the driver outlives the caller's runtime. A caller that runs a pull on a
/// runtime it later drops passes a long-lived one here: a driver dropped with its
/// runtime before the connection reaches QUIC's draining state leaves the
/// connection in the endpoint's active set for good, and the endpoint's close then
/// waits for it forever. With `None`, the dial and the driver run inline on the
/// caller's runtime.
///
/// The spawned dial carries the caller's current span, so its connect events and
/// the driver's stay in the caller's trace. Dropping the returned future mid-dial
/// aborts the spawned dial.
///
/// # Errors
///
/// A connect or transport fault, as `stage: {err}` (a rate-limit shed keeps its
/// typed sentinel). A spawned dial that panics, or that `runtime` cancels because
/// it is shutting down, fails as a [`LocalPullFault`]: the dial runtime is this
/// node's, so neither outcome says anything about the peer.
pub(crate) async fn dial(
    endpoint: &Endpoint,
    target: EndpointAddr,
    runtime: Option<&Handle>,
    stage: &'static str,
) -> anyhow::Result<Connection> {
    let Some(runtime) = runtime else {
        return endpoint
            .connect(target, ALPN_CLIENT)
            .await
            .map_err(|e| rate_limited::transport_error(stage, e));
    };
    let endpoint = endpoint.clone();
    let connect = async move { endpoint.connect(target, ALPN_CLIENT).await }
        .instrument(tracing::Span::current());
    let mut task = AbortOnDrop(runtime.spawn(connect));
    (&mut task.0)
        .await
        .map_err(|e| {
            let outcome = if e.is_panic() {
                "panicked"
            } else {
                "was cancelled by its runtime shutting down"
            };
            anyhow::anyhow!("{stage}: the dial task {outcome}").context(LocalPullFault)
        })?
        .map_err(|e| rate_limited::transport_error(stage, e))
}

/// Aborts the spawned task when dropped, so a dial the caller stops waiting for
/// stops too.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A dialed `cdn/client/v1` connection reused across many hash fetches.
///
/// Each fetch opens a new bi-stream on the SAME connection (one stream = one
/// hash, no multiplexing), amortizing the dial over every hash. The connection is
/// closed exactly once, on this handle's own [`Drop`]; a per-hash pull that
/// borrows the connection (`open_progressive_pull_on`) tears down only
/// its own stream and leaves the connection open for the next hash.
#[derive(Debug)]
pub struct WarmConnection {
    /// The live QUIC connection. Cloned into each pull, which leaves it open on
    /// its own teardown; this handle owns the single close.
    conn: Connection,
}

impl WarmConnection {
    /// Dial `target` on the CDN client ALPN and keep the connection warm.
    ///
    /// Bounded as a whole by `open`, the same dial budget
    /// [`crate::open_progressive_pull`] applies to its one-shot dial.
    /// `dial_runtime` is the runtime the dial and the connection's driver run on;
    /// `None` runs them on the caller's (see
    /// [`PeerSource::with_dial_runtime`](crate::source::PeerSource::with_dial_runtime)).
    ///
    /// # Errors
    ///
    /// A connect / transport fault, `open` elapsing before the connection is
    /// established ([`PullTimeout`]), or a spawned dial that panicked or was
    /// cancelled by `dial_runtime` shutting down ([`LocalPullFault`]).
    pub async fn connect(
        endpoint: &Endpoint,
        target: EndpointAddr,
        open: Duration,
        dial_runtime: Option<&Handle>,
    ) -> anyhow::Result<Self> {
        let conn = tokio::time::timeout(
            open,
            dial(endpoint, target, dial_runtime, "warm connect failed"),
        )
        .await
        .map_err(|_| anyhow::Error::new(PullTimeout { after: open }))??;
        Ok(Self { conn })
    }

    /// The live connection, for opening a per-hash bi-stream.
    pub(crate) const fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Whether the connection has closed: by either side, by an idle timeout,
    /// or by a transport fault. A closed connection opens no more streams.
    pub(crate) fn is_closed(&self) -> bool {
        self.conn.close_reason().is_some()
    }
}

/// One live `cdn/client/v1` connection per node, shared by every pull of one
/// command.
///
/// A pull opens its own bi-stream on the node's connection and dials only when
/// the node has no live connection. Concurrent callers for one node share one
/// dial. A closed connection is dialled again on the next [`get`](Self::get),
/// so a transport fault (a reset, a closed connection, an idle timeout) costs
/// one redial. A protocol refusal or a hash mismatch ends only its own stream,
/// and the connection stays. The connection carries no payment state: payment
/// lanes stay per (signer, provider).
///
/// Cloning is cheap: every clone shares one map. Each connection closes when
/// the map drops it and no pull still holds it.
///
/// **One long-lived runtime only (#1675).** A connection's QUIC driver lives on
/// the runtime that dialled it, and the connection makes progress only while
/// that driver runs. Share a map only between pulls on one runtime that
/// outlives the map, such as the `decdn` CLI's. A caller that pulls on
/// runtimes it throws away (`decdn-node`'s per-serve pull legs) dials per leg
/// instead, on a long-lived dial runtime
/// ([`PeerSource::with_dial_runtime`](crate::source::PeerSource::with_dial_runtime)).
#[derive(Debug, Clone)]
pub struct Connections {
    inner: Arc<ConnectionsInner>,
}

/// A node's connection slot. The async lock makes concurrent callers for one
/// node wait on one dial.
type Slot = Arc<tokio::sync::Mutex<Option<Arc<WarmConnection>>>>;

#[derive(Debug)]
struct ConnectionsInner {
    endpoint: Endpoint,
    slots: Mutex<HashMap<PublicKey, Slot>>,
    /// How many dials this map has made.
    #[cfg(any(test, feature = "test-util"))]
    dials: std::sync::atomic::AtomicU64,
}

impl Connections {
    /// An empty map that dials from `endpoint`.
    #[must_use]
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            inner: Arc::new(ConnectionsInner {
                endpoint,
                slots: Mutex::new(HashMap::new()),
                #[cfg(any(test, feature = "test-util"))]
                dials: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    /// `target`'s live connection, dialled on the caller's runtime when the
    /// node has none. A caller for a node another caller is dialling waits
    /// for that dial and shares its connection. The dial has no deadline of
    /// its own: the caller bounds it.
    ///
    /// # Errors
    ///
    /// A connect or transport fault from the dial, as
    /// `connect failed: {err}` (a rate-limit shed keeps its typed sentinel).
    pub async fn get(&self, target: &EndpointAddr) -> anyhow::Result<Arc<WarmConnection>> {
        let slot = Arc::clone(
            self.inner
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(target.id)
                .or_default(),
        );
        let mut held = slot.lock().await;
        if let Some(warm) = held.as_ref().filter(|warm| !warm.is_closed()) {
            return Ok(Arc::clone(warm));
        }
        #[cfg(any(test, feature = "test-util"))]
        self.inner
            .dials
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let conn = dial(&self.inner.endpoint, target.clone(), None, "connect failed").await?;
        let warm = Arc::new(WarmConnection { conn });
        *held = Some(Arc::clone(&warm));
        Ok(warm)
    }

    /// Drop `node`'s connection if it has closed, so its resources go at once
    /// rather than at the next [`get`](Self::get). A live connection stays:
    /// another caller may have dialled it after the fault, and its pulls run
    /// on it.
    pub fn invalidate(&self, node: PublicKey) {
        let slot = self
            .inner
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&node)
            .map(Arc::clone);
        // A slot that is locked is mid-dial, and the dial replaces what it
        // holds.
        if let Some(slot) = slot
            && let Ok(mut held) = slot.try_lock()
            && held.as_ref().is_some_and(|warm| warm.is_closed())
        {
            *held = None;
        }
    }

    /// How many dials this map has made.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn dials(&self) -> u64 {
        self.inner.dials.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for WarmConnection {
    /// Close the warm connection once the caller is done with it. `Connection::close`
    /// is first-wins and idempotent, and every per-hash pull leaves the connection
    /// open, so this is the single teardown for the whole warm connection.
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"warm-connection-dropped");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests {
    use std::time::Duration;

    use iroh::endpoint::presets;
    use iroh::{Endpoint, EndpointAddr, RelayMode};
    use tokio::runtime::Handle;

    use decdn_protocol::ALPN_CLIENT;

    /// Bind a loopback endpoint with relays off, returning it and a dialable
    /// address.
    async fn loopback_endpoint(alpns: Vec<Vec<u8>>) -> (Endpoint, EndpointAddr) {
        let ep = Endpoint::builder(presets::Minimal)
            .alpns(alpns)
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))
            .expect("loopback bind address")
            .bind()
            .await
            .expect("bind loopback endpoint");
        let socket = ep
            .bound_sockets()
            .into_iter()
            .find(std::net::SocketAddr::is_ipv4)
            .expect("an IPv4 bound socket");
        let addr = EndpointAddr::new(ep.id()).with_ip_addr(socket);
        (ep, addr)
    }

    /// How the throwaway runtime dials.
    #[derive(Clone, Copy)]
    enum Via {
        /// A one-shot [`super::dial`].
        OneShot,
        /// A [`super::WarmConnection::connect`].
        Warm,
    }

    /// Dial a peer that holds its side open, from a throwaway current-thread
    /// runtime on its own thread, then drop that runtime — the shape of a
    /// `decdn-node` pull leg. `dial_runtime` is passed through to the dial.
    /// Returns the dialling endpoint, the dialled handle (still held, so the
    /// connection is still open), and the peer endpoint, which must outlive the
    /// test's close.
    async fn dial_from_a_dropped_runtime(
        via: Via,
        dial_runtime: Option<Handle>,
    ) -> (Endpoint, Box<dyn std::any::Any + Send>, Endpoint) {
        let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
        let accept_peer = peer.clone();
        tokio::spawn(async move {
            if let Some(incoming) = accept_peer.accept().await
                && let Ok(connecting) = incoming.accept()
            {
                let _held = connecting.await;
                std::future::pending::<()>().await;
            }
        });
        let (ep, _) = loopback_endpoint(Vec::new()).await;
        let dialer = ep.clone();
        let conn = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build throwaway runtime");
            let held: Box<dyn std::any::Any + Send> = match via {
                Via::OneShot => Box::new(
                    rt.block_on(super::dial(
                        &dialer,
                        peer_addr,
                        dial_runtime.as_ref(),
                        "connect failed",
                    ))
                    .expect("dial the peer"),
                ),
                Via::Warm => Box::new(
                    rt.block_on(super::WarmConnection::connect(
                        &dialer,
                        peer_addr,
                        Duration::from_secs(10),
                        dial_runtime.as_ref(),
                    ))
                    .expect("warm-dial the peer"),
                ),
            };
            drop(rt);
            held
        })
        .join()
        .expect("dialling thread");
        (ep, conn, peer)
    }

    /// A dial run on a long-lived runtime leaves the connection's driver there,
    /// so the connection drains after the dialling runtime is gone and the
    /// endpoint closes promptly (#2185).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dial_on_a_long_lived_runtime_lets_the_endpoint_close() {
        let (ep, _conn, _peer) =
            dial_from_a_dropped_runtime(Via::OneShot, Some(Handle::current())).await;
        tokio::time::timeout(Duration::from_secs(10), ep.close())
            .await
            .expect("the endpoint must close once its connection drains");
    }

    /// The warm connection's dial honours the dial runtime the same way.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_warm_dial_on_a_long_lived_runtime_lets_the_endpoint_close() {
        let (ep, _warm, _peer) =
            dial_from_a_dropped_runtime(Via::Warm, Some(Handle::current())).await;
        tokio::time::timeout(Duration::from_secs(10), ep.close())
            .await
            .expect("the endpoint must close once its warm connection drains");
    }

    /// A dial whose runtime is shutting down fails as this node's fault, not the
    /// peer's, and says the task was cancelled rather than that it panicked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dial_on_a_shut_down_runtime_is_a_local_fault() {
        let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
        let (ep, _) = loopback_endpoint(Vec::new()).await;
        let gone = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build the dial runtime");
        let handle = gone.handle().clone();
        gone.shutdown_background();

        let err = tokio::time::timeout(
            Duration::from_secs(10),
            super::dial(&ep, peer_addr, Some(&handle), "connect failed"),
        )
        .await
        .expect("a dial on a shut-down runtime must fail, not hang")
        .expect_err("a dial on a shut-down runtime must fail");
        assert!(
            err.downcast_ref::<crate::LocalPullFault>().is_some(),
            "the failure must be marked local: {err:#}"
        );
        assert!(
            format!("{err:#}").contains("cancelled"),
            "the failure must say the task was cancelled: {err:#}"
        );
        drop(peer);
    }

    /// Dropping the guard aborts the task it holds, so a dial the caller stops
    /// waiting for (an `open` timeout) does not run on to completion.
    #[tokio::test]
    async fn dropping_the_guard_aborts_its_task() {
        let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
        let guard = super::AbortOnDrop(tokio::spawn(async move {
            let _held = held_tx;
            std::future::pending::<()>().await;
        }));
        drop(guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), held_rx)
                .await
                .expect("the aborted task must drop its state promptly")
                .is_err(),
            "the task must end by abort, never by sending"
        );
    }

    /// A peer that accepts every connection and holds each one open, counting
    /// them. Returns its address and the count.
    async fn holding_peer() -> (
        Endpoint,
        EndpointAddr,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        let (peer, peer_addr) = loopback_endpoint(vec![ALPN_CLIENT.to_vec()]).await;
        let accepted = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (accept_peer, count) = (peer.clone(), std::sync::Arc::clone(&accepted));
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Some(incoming) = accept_peer.accept().await {
                if let Ok(connecting) = incoming.accept()
                    && let Ok(conn) = connecting.await
                {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    held.push(conn);
                }
            }
        });
        (peer, peer_addr, accepted)
    }

    /// Wait until the peer has accepted `n` connections, then check it
    /// accepts no more for a moment.
    async fn assert_accepted(accepted: &std::sync::atomic::AtomicU64, n: u64) {
        let load = || accepted.load(std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), async {
            while load() < n {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the peer accepts the dials");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(load(), n, "the peer accepts exactly {n} connections");
    }

    /// Two gets for one node dial once and share the connection, and two
    /// concurrent gets share one dial.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_gets_for_one_node_dial_once() {
        let (_peer, peer_addr, accepted) = holding_peer().await;
        let (ep, _) = loopback_endpoint(Vec::new()).await;
        let connections = super::Connections::new(ep.clone());

        let (a, b) = tokio::join!(connections.get(&peer_addr), connections.get(&peer_addr));
        let (a, b) = (a.expect("first get"), b.expect("second get"));
        let c = connections.get(&peer_addr).await.expect("third get");
        assert!(std::sync::Arc::ptr_eq(&a, &b) && std::sync::Arc::ptr_eq(&a, &c));
        assert_eq!(connections.dials(), 1, "one dial for three gets");
        assert_accepted(&accepted, 1).await;
    }

    /// A get after the node's connection closes dials again, and invalidate
    /// drops only a closed connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_get_after_the_connection_closes_dials_again() {
        let (_peer, peer_addr, accepted) = holding_peer().await;
        let (ep, _) = loopback_endpoint(Vec::new()).await;
        let connections = super::Connections::new(ep.clone());

        let first = connections.get(&peer_addr).await.expect("first get");
        connections.invalidate(peer_addr.id);
        let kept = connections
            .get(&peer_addr)
            .await
            .expect("get after invalidate");
        assert!(
            std::sync::Arc::ptr_eq(&first, &kept),
            "invalidate keeps a live connection"
        );
        assert_eq!(connections.dials(), 1);

        first.connection().close(0u32.into(), b"test-closed");
        assert!(first.is_closed());
        connections.invalidate(peer_addr.id);
        let second = connections.get(&peer_addr).await.expect("get after close");
        assert!(!std::sync::Arc::ptr_eq(&first, &second));
        assert!(!second.is_closed());
        assert_eq!(connections.dials(), 2, "a closed connection redials");
        assert_accepted(&accepted, 2).await;
    }

    /// The control: the same dial on the dropped runtime itself takes the
    /// connection's driver down with it, and the endpoint's close never
    /// finishes. Without it, the test above would pass for a setup that never
    /// stranded anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dial_on_a_dropped_runtime_blocks_the_endpoint_close() {
        let (ep, _conn, _peer) = dial_from_a_dropped_runtime(Via::OneShot, None).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(2), ep.close())
                .await
                .is_err(),
            "a connection whose driver was dropped must keep the close waiting"
        );
    }
}
