//! Connection-limiter wrapper for foreign `ProtocolHandler` implementations.
//!
//! deCDN-authored handlers (e.g.
//! [`probe::ProbeHandler`](crate::handlers::probe::ProbeHandler)) call
//! [`ConnectionLimiter::acquire`] inline at the top of their `accept`
//! implementations. Foreign handlers — `iroh-gossip`'s [`Gossip`] today, and
//! any future third-party `ProtocolHandler` we attach to the iroh `Router` —
//! cannot be modified to do the same. [`LimitedHandler`] is a thin wrapper
//! that gates `accept` on the shared [`ConnectionLimiter`] and only delegates
//! to the inner handler if a permit is granted.
//!
//! Without this wrapper a connection flood on a foreign ALPN bypasses
//! `ConnectionLimiter` entirely (issue #433): the global semaphore introduced
//! in #235 bounds `accept` invocations on probe but not on gossip, so the
//! per-task resource ceiling does not hold network-wide.
//!
//! Probe stays on its inline `acquire` call: it has a custom per-rejection-
//! layer close-frame timeout policy that this generic adapter intentionally
//! does not replicate (under `GlobalFull` a flood-wide `conn.closed()` wait
//! re-introduces the memory-pressure path the limiter exists to prevent).
//!
//! [`Gossip`]: iroh_gossip::net::Gossip

use std::sync::Arc;

use iroh::endpoint::{Accepting, Connection, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};

use decdn_protocol::APP_ERR_RATE_LIMITED;

use crate::dispatch::ConnectionLimiter;

/// Wrap a [`ProtocolHandler`] so its `accept` is gated by [`ConnectionLimiter`].
///
/// `H` is held by value because typical inner handlers (notably `iroh-gossip`'s
/// `Gossip`) are themselves `Arc`-backed `Clone` types — wrapping them in
/// another `Arc` would only add an indirection.
pub struct LimitedHandler<H: ProtocolHandler> {
    inner: H,
    limiter: Arc<ConnectionLimiter>,
}

impl<H: ProtocolHandler> LimitedHandler<H> {
    /// Wrap `inner` with `limiter`. Construction is cheap: only an `H`
    /// (typically itself a cheap `Clone`) and an `Arc` clone are stored.
    #[must_use = "constructing a LimitedHandler and dropping it does nothing — \
                  attach it to a Router::builder via .accept(...)"]
    #[allow(clippy::missing_const_for_fn)] // generic field assignment isn't const-stable.
    pub fn new(inner: H, limiter: Arc<ConnectionLimiter>) -> Self {
        Self { inner, limiter }
    }
}

// `ConnectionLimiter` deliberately omits `Debug`; render it opaquely so the
// trait's `Debug` bound is satisfied without leaking limiter internals.
impl<H: ProtocolHandler> std::fmt::Debug for LimitedHandler<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LimitedHandler")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<H: ProtocolHandler> ProtocolHandler for LimitedHandler<H> {
    /// Forward the accept-time interception point. The trait default just
    /// awaits the `Accepting` future, which is also what most handlers do —
    /// but forwarding makes the wrapper transparent to any inner handler that
    /// implements `on_accepting` for early rejection or 0-RTT setup.
    async fn on_accepting(&self, accepting: Accepting) -> Result<Connection, AcceptError> {
        self.inner.on_accepting(accepting).await
    }

    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let _permit = match self.limiter.acquire(&conn) {
            Ok(p) => p,
            Err(reason) => {
                // Rate-limited rejection is normal load-shedding, not a
                // protocol fault: returning `Err` here would have iroh log
                // every rejection as an `AcceptError`, amplifying log volume
                // under flood (the exact attack outcome the limiter prevents).
                // The dispatch layer already emits a structured debug log and
                // a metric counter for the rejection.
                conn.close(
                    VarInt::from_u32(APP_ERR_RATE_LIMITED),
                    reason.as_str().as_bytes(),
                );
                // No `conn.closed()` wait. Probe waits briefly on `PerSource`
                // rejections so the peer reliably observes the layer-label
                // byte; for a generic foreign-handler wrapper we don't own
                // the protocol semantics, and on `GlobalFull` a wait would
                // park each rejected task for up to the timeout — at
                // thousands of rejections per second that is the
                // memory-pressure path the limiter exists to prevent.
                return Ok(());
            }
        };
        self.inner.accept(conn).await
        // `_permit` drops at end of scope (after the inner `accept` returns),
        // keeping the in-flight gauge balanced against the iroh-spawned task
        // lifetime.
    }

    /// Forward shutdown so iroh's drain semantics propagate to the inner
    /// handler. The trait default is a no-op, but `iroh-gossip`'s `Gossip`
    /// overrides `shutdown` to send `Disconnect` messages to peers and stop
    /// the gossip actor — a wrapper that did not forward would silently
    /// break graceful drain.
    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iroh::endpoint::Connection;
    use iroh::protocol::{AcceptError, ProtocolHandler};

    use super::LimitedHandler;
    use crate::dispatch::ConnectionLimiter;
    use crate::metrics::Metrics;
    use decdn_common::config::ResolvedSecurity;

    /// Inner test handler whose only job is to record how many times each
    /// `ProtocolHandler` method was invoked. `Clone` is cheap (every field is
    /// `Arc`-backed) so the test can keep an inspector clone after handing
    /// one into [`LimitedHandler::new`].
    #[derive(Debug, Clone, Default)]
    struct CountingHandler {
        accepts: Arc<AtomicUsize>,
        shutdowns: Arc<AtomicUsize>,
    }

    impl ProtocolHandler for CountingHandler {
        async fn accept(&self, _conn: Connection) -> Result<(), AcceptError> {
            self.accepts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn shutdown(&self) {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn permissive_security() -> ResolvedSecurity {
        ResolvedSecurity {
            max_concurrent_handlers: u32::MAX,
            per_source_rate_per_sec: 1e9,
            per_source_burst: u32::MAX,
            max_tracked_sources: 4096,
        }
    }

    /// `LimitedHandler::shutdown` must forward to the inner handler. iroh-
    /// gossip overrides `shutdown` to send `Disconnect` messages and stop
    /// the gossip actor, so a wrapper that missed the call would silently
    /// break graceful drain on `Router::shutdown`.
    #[tokio::test]
    async fn shutdown_forwards_to_inner() {
        let metrics = Arc::new(Metrics::new());
        let limiter = Arc::new(ConnectionLimiter::new(&permissive_security(), metrics));
        let inspector = CountingHandler::default();
        let handler = LimitedHandler::new(inspector.clone(), limiter);

        handler.shutdown().await;

        assert_eq!(
            inspector.shutdowns.load(Ordering::SeqCst),
            1,
            "wrapper must invoke inner.shutdown exactly once"
        );
    }
}
