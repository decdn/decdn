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
use tokio_util::task::AbortOnDropHandle;
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
    AbortOnDropHandle::new(runtime.spawn(connect))
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

/// A dialed `cdn/client/v1` connection reused across many hash fetches.
///
/// Each fetch opens a new bi-stream on the SAME connection (one stream = one
/// hash, no multiplexing), amortizing the dial over every hash. The connection is
/// closed exactly once, on this handle's own [`Drop`]; a per-hash pull that
/// borrows the connection (`open_progressive_pull_on`) tears down only
/// its own stream and leaves the connection open for the next hash.
#[derive(Debug)]
#[doc(hidden)]
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
/// dial. A pull that meets a transport fault (a connection reset, a closed
/// connection, an idle timeout, an open or read that falls under its
/// deadline) unpins its connection ([`invalidate`](Self::invalidate)), so the
/// next [`get`](Self::get) dials again. A protocol refusal or a hash mismatch
/// ends only its own stream, and the connection stays pinned. The connection
/// carries no payment state: payment lanes stay per (signer, provider).
///
/// Cloning is cheap: every clone shares one map. Each pull holds its
/// connection, so each connection closes when the map drops it and no pull
/// still holds it.
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
    #[doc(hidden)]
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

    /// Unpin `conn` from `node`'s slot, so the next [`get`](Self::get) dials
    /// again. Only that instance is unpinned: when another caller has already
    /// dialled a newer connection into the slot, the newer one stays. Pulls
    /// that still hold `conn` keep it; it closes when the last of them ends.
    #[doc(hidden)]
    pub async fn invalidate(&self, node: PublicKey, conn: &Arc<WarmConnection>) {
        let slot = self
            .inner
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&node)
            .map(Arc::clone);
        let Some(slot) = slot else {
            return;
        };
        let mut held = slot.lock().await;
        if held
            .as_ref()
            .is_some_and(|pinned| Arc::ptr_eq(pinned, conn))
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

/// Whether `err` on a pull over `warm` is a transport fault: the connection
/// has closed (a connection reset, a close by either side, an idle timeout, a
/// lost connection under a read or an `open_bi`), or the open or a read fell
/// under its deadline ([`PullTimeout`], [`PullStalled`](crate::PullStalled)).
/// A refusal, a verify or hash fault, and a stream-level reset (a rate-limit
/// shed of one stream) are not.
pub(crate) fn is_transport_fault(err: &anyhow::Error, warm: &WarmConnection) -> bool {
    warm.is_closed()
        || err
            .chain()
            .any(|e| e.is::<PullTimeout>() || e.is::<crate::PullStalled>())
}

/// What a pull owns of its connection, which sets its teardown.
#[derive(Debug, Clone)]
pub(crate) enum ConnOwner {
    /// A one-shot dial: the pull closes the connection on teardown.
    Owned,
    /// A caller-owned [`WarmConnection`]: the pull tears down only its own
    /// stream, and the warm connection closes once, on its own `Drop`.
    #[cfg(any(test, feature = "test-util"))]
    Borrowed,
    /// A connection from a [`Connections`] map: the pull tears down only its
    /// own stream, holds the connection until it ends, and unpins it from
    /// `map` on a transport fault.
    Shared {
        /// The connection, held for the pull's life.
        warm: Arc<WarmConnection>,
        /// The map it is pinned in.
        map: Connections,
    },
}

impl ConnOwner {
    /// Whether the pull closes the connection on teardown.
    pub(crate) const fn owns(&self) -> bool {
        matches!(self, Self::Owned)
    }

    /// Unpin a shared connection from its map when `err` is a transport
    /// fault ([`is_transport_fault`]). Any other owner, and any other error,
    /// leaves the map as it is.
    pub(crate) async fn unpin_on_fault(&self, err: &anyhow::Error) {
        if let Self::Shared { warm, map } = self
            && is_transport_fault(err, warm)
        {
            map.invalidate(warm.connection().remote_id(), warm).await;
        }
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
mod tests;
