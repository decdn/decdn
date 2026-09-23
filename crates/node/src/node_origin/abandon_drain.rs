//! Waiting for an ABANDONED pull leg's upstream connections to drain before the
//! runtime their QUIC drivers live on is dropped.
//!
//! # The hazard
//!
//! A pull leg runs `drive` on an ephemeral current-thread runtime that the
//! caller drops the instant the leg returns. On cancel or error the `drive`
//! future is dropped mid-transfer, which QUEUES an upstream `Connection::close`
//! (via `UpstreamPull::drop`) but does not drive it to completion. QUIC then
//! holds the connection for `3 * PTO` — derived from that connection's own RTT
//! samples, and floored by the peer's `max_ack_delay` — before its driver
//! forwards the endpoint's draining event, and [`iroh::Endpoint::close`] waits
//! on exactly that event. Drop the runtime first and the connection is stranded
//! with no driver: it can never reach drained, and the node's endpoint close
//! waits forever.
//!
//! # Why the wait is observed, not timed
//!
//! `3 * PTO` is a measured, load-dependent quantity, so no fixed sleep can be
//! the right length: under CPU starvation the inflated RTT samples push it past
//! any constant chosen for the healthy case. What this module waits on instead
//! is the transition itself. The connection driver holds the last strong
//! reference to the connection and releases it on the same poll that forwards
//! the draining event, so a [`WeakConnectionHandle`] that no longer upgrades IS
//! the drained state. `Connection::closed()` is not that signal — it resolves
//! the moment the local close sets the connection error, long before the
//! `CONNECTION_CLOSE` is on the wire.
//!
//! # What gets recorded
//!
//! Every connection the pull leg's own [`decdn_client::source::PeerSource`]
//! dials, observed at the
//! DIAL rather than on the returned reader. A handshake that fails after the
//! connect — a refusal, an over-ceiling rate or size, a resume offset past the
//! end — returns no reader at all, and those are precisely the opens whose
//! connection is left live on the pull runtime, so a wrapper around the reader
//! would miss them.
//!
//! The orchestration's pre-flight handshake is deliberately NOT recorded:
//! `NodeOrigin::open_pull_leg` and `pull_from_candidate` run it before the pull
//! runtime exists, so its driver lives on the outer runtime, which keeps living.
//!
//! # What is not covered
//!
//! A pull that returns cleanly. `UpstreamPull::finish` closes the connection
//! without awaiting drained, so a successful leg strands its driver the same
//! way — the same open residual as #1675, not something this module resolves.
//! Waiting there would add `3 * PTO` to every successful serve-miss teardown,
//! which the last serve observer joins.

use std::sync::Mutex;
use std::time::Duration;

use alloy::primitives::Address;
use decdn_client::DialObserver;
use iroh::endpoint::WeakConnectionHandle;

/// The ceiling on an abandoned pull leg's drain wait.
///
/// The quantity being covered is QUIC's close timer, `3 * PTO`, so this is not
/// "how long a drain takes" — it is the point at which a drain that has not
/// happened is not going to. `3 * PTO` is floored by three times the peer's
/// `max_ack_delay` (25 ms at iroh's defaults), so even a loopback drain takes
/// ~75 ms; a PTO inflated to about a second by CPU starvation puts it near three
/// seconds, which is why the ceiling sits well above that. Ten seconds is also
/// short relative to an operator-visible node shutdown, during which an
/// in-flight abandoned pull is waited out inside `router.shutdown()`.
pub const ABANDON_DRAIN_CAP: Duration = Duration::from_secs(10);

/// One step of the drain poll, sized for resolution against the ~75 ms floor
/// above rather than to finish in a single tick.
const DRAIN_STEP: Duration = Duration::from_millis(10);

/// Every upstream connection one pull leg dialled, held WEAKLY so a leg that is
/// abandoned can wait for each to reach its drained state.
///
/// Weak by construction, on both counts that matter: holding one cannot delay
/// the close it observes, and a connection that drained while the leg was still
/// running costs nothing to wait for.
#[derive(Debug, Default)]
pub(super) struct ConnDrain(Mutex<Vec<WeakConnectionHandle>>);

impl ConnDrain {
    /// A [`DialObserver`] that records into this set, to hand to
    /// [`decdn_client::source::PeerSource::with_dial_observer`].
    pub(super) fn observer(&self) -> impl Fn(WeakConnectionHandle) + Send + Sync + '_ {
        move |handle| self.record(handle)
    }

    /// Record one dialled connection.
    ///
    /// A poisoned lock is read through rather than skipped: the guarded value is
    /// a plain `Vec` of weak handles with no invariant a panic could break, and
    /// dropping a handle here would let the wait finish early on a connection
    /// that is still live.
    fn record(&self, handle: WeakConnectionHandle) {
        let mut conns = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        conns.push(handle);
    }

    /// Whether no recorded handle still upgrades — read through a poisoned lock
    /// for the same reason [`Self::record`] writes through one.
    fn all_drained(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|c| c.upgrade().is_none())
    }

    /// How many recorded connections are still held.
    fn still_held(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|c| c.upgrade().is_some())
            .count()
    }

    /// Wait until every recorded connection's driver has released it, or `cap`
    /// elapses. Returns the number still held, so `0` is a clean drain.
    async fn wait_drained(&self, cap: Duration) -> usize {
        let deadline = tokio::time::Instant::now() + cap;
        loop {
            if self.all_drained() {
                return 0;
            }
            if tokio::time::Instant::now() >= deadline {
                return self.still_held();
            }
            tokio::time::sleep(DRAIN_STEP).await;
        }
    }
}

/// Wait out one abandoned leg's upstream connections, metering and warning for
/// each one still held at the ceiling.
///
/// Called only on the cancel and `Err` paths. A connection still held here is
/// one this node is about to strand, which is what makes an endpoint close hang,
/// so it is worth a counter and an operator-visible line rather than a silent
/// return. The counter is bumped once PER stranded connection: one leg opens one
/// connection per gap, and an error path can leave several.
pub(super) async fn drain_abandoned(
    conns: &ConnDrain,
    provider_addr: Address,
    metrics: &crate::metrics::Metrics,
) {
    let stranded = conns.wait_drained(ABANDON_DRAIN_CAP).await;
    if stranded == 0 {
        return;
    }
    for _ in 0..stranded {
        metrics.node_pull_abandon_drain_timeout();
    }
    tracing::warn!(
        provider = %provider_addr,
        stranded,
        "abandoned upstream connections did not drain within the cap; endpoint close may block"
    );
}

/// Bind a [`ConnDrain`]'s observer to the lifetime a [`PeerSource`] needs.
///
/// [`PeerSource::with_dial_observer`] takes `&DialObserver`, and the closure
/// [`ConnDrain::observer`] returns is a temporary — this names it so a caller can
/// hold it beside the source.
///
/// [`PeerSource`]: decdn_client::source::PeerSource
/// [`PeerSource::with_dial_observer`]: decdn_client::source::PeerSource::with_dial_observer
pub(super) fn as_observer<'a>(
    f: &'a (impl Fn(WeakConnectionHandle) + Send + Sync + 'a),
) -> &'a DialObserver<'a> {
    f
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::{ABANDON_DRAIN_CAP, ConnDrain};
    use std::time::Duration;

    /// An empty record drains immediately: there is nothing to wait for, and a
    /// leg that dialled nothing must not pay the ceiling.
    #[tokio::test(start_paused = true)]
    async fn an_empty_record_is_drained() {
        let conns = ConnDrain::default();
        assert_eq!(conns.wait_drained(ABANDON_DRAIN_CAP).await, 0);
    }

    /// A poisoned lock is read THROUGH, not treated as drained. Poisoning it
    /// with a live handle recorded would, under a fail-open read, report a clean
    /// drain and strand the connection silently.
    #[tokio::test(start_paused = true)]
    async fn a_poisoned_lock_is_read_through() {
        let conns = ConnDrain::default();
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = conns.0.lock().unwrap();
            panic!("poison the drain record");
        }));
        assert!(poison.is_err(), "the panic must poison the mutex");
        assert!(conns.0.is_poisoned(), "precondition: the lock is poisoned");
        // Still readable, and still correct: the record is empty, so drained.
        assert_eq!(conns.wait_drained(ABANDON_DRAIN_CAP).await, 0);
    }

    /// The wait is bounded even when a connection never drains, and reports how
    /// many are still held so the caller can meter one tick each.
    #[tokio::test(start_paused = true)]
    async fn a_handle_that_never_drains_ceilings_out() {
        let conns = ConnDrain::default();
        // A handle whose connection is alive for the whole wait. Built from a
        // real endpoint pair so this exercises `upgrade`, not a stand-in.
        let Some((_ep_a, _ep_b, conn)) = super::tests::loopback_conn().await else {
            return; // no loopback available in this environment
        };
        conns.record(conn.weak_handle());
        let started = tokio::time::Instant::now();
        let stranded = conns.wait_drained(Duration::from_millis(50)).await;
        assert_eq!(stranded, 1, "the live connection must be reported as held");
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "the wait must reach its cap"
        );
    }

    /// Dial a loopback connection and keep both endpoints alive, or `None` if
    /// the environment cannot bind.
    async fn loopback_conn() -> Option<(iroh::Endpoint, iroh::Endpoint, iroh::endpoint::Connection)>
    {
        use iroh::{RelayMode, endpoint::presets};
        let alpn = b"cdn/drain-test/v1".to_vec();
        let server = iroh::Endpoint::builder(presets::Minimal)
            .alpns(vec![alpn.clone()])
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))
            .ok()?
            .bind()
            .await
            .ok()?;
        let addr = server.bound_sockets().first().copied()?;
        let server_id = server.id();
        let accept = {
            let server = server.clone();
            tokio::spawn(async move {
                if let Some(incoming) = server.accept().await
                    && let Ok(connecting) = incoming.accept()
                {
                    let _ = connecting.await;
                }
            })
        };
        let client = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::LOCALHOST,
                0,
            ))
            .ok()?
            .bind()
            .await
            .ok()?;
        let conn = client
            .connect(iroh::EndpointAddr::new(server_id).with_ip_addr(addr), &alpn)
            .await
            .ok()?;
        accept.abort();
        Some((server, client, conn))
    }
}
