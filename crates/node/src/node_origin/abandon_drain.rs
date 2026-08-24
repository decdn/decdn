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
//! samples — before its driver forwards the endpoint's draining event, and
//! [`iroh::Endpoint::close`] waits on exactly that event. Drop the runtime
//! first and the connection is stranded with no driver: it can never reach
//! drained, and the node's endpoint close waits forever.
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
//! # What needs draining
//!
//! Only the connections `drive` opens. The header handshake
//! ([`decdn_client_pull::open_progressive_pull`]) runs on the OUTER runtime,
//! which keeps living, so its close is driven to completion by a runtime that
//! outlives it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::Address;

use decdn_bao_range::AlignedRange;
use decdn_client_pull::sink::PullReader;
use decdn_client_pull::source::{BlobSource, PeerSource, SourceFuture};
use decdn_client_pull::{UpstreamPullHeader, VoucherProgress};
use iroh::endpoint::WeakConnectionHandle;

/// The ceiling on an abandoned pull leg's drain wait.
///
/// The quantity being covered is QUIC's close timer, `3 * PTO`, so this is not
/// "how long a drain takes" — it is the point at which a drain that has not
/// happened is not going to. On loopback the wait returns in a single
/// [`DRAIN_STEP`]. A PTO inflated to about a second by CPU starvation puts
/// `3 * PTO` near three seconds, which is why the ceiling sits well above it. It
/// stays under the node's own shutdown deadline, so a shutdown that catches an
/// in-flight abandoned pull still completes inside its budget.
pub(super) const ABANDON_DRAIN_CAP: Duration = Duration::from_secs(10);

/// One step of the drain poll. Small enough that the common case — a loopback or
/// LAN peer whose driver finishes almost at once — costs one step.
const DRAIN_STEP: Duration = Duration::from_millis(10);

/// Every upstream connection one pull leg opened, held WEAKLY so a leg that is
/// abandoned can wait for each to reach its drained state.
///
/// Weak by construction, on both counts that matter: holding one cannot delay
/// the close it observes, and a connection that drained while the leg was still
/// running costs nothing to wait for.
#[derive(Debug, Clone, Default)]
pub(super) struct ConnDrain(Arc<Mutex<Vec<WeakConnectionHandle>>>);

impl ConnDrain {
    /// Record one opened connection. A poisoned lock drops the handle: this is a
    /// teardown hint, and losing one costs at most the endpoint-close hazard the
    /// wait exists to avoid — never correctness of the pull itself.
    fn record(&self, handle: WeakConnectionHandle) {
        if let Ok(mut conns) = self.0.lock() {
            conns.push(handle);
        }
    }

    /// Wait until every recorded connection's driver has released it, or `cap`
    /// elapses.
    ///
    /// Returns `false` on the ceiling. That is the caller's cue to meter and
    /// warn: a connection still held here is one this node is about to strand,
    /// and a stranded connection is what makes an endpoint close hang.
    pub(super) async fn wait_drained(&self, cap: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + cap;
        loop {
            if self.all_drained() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(DRAIN_STEP).await;
        }
    }

    /// Whether no recorded handle still upgrades. A poisoned lock reports drained:
    /// there is nothing left to observe through it, and blocking teardown on a
    /// poisoned mutex would trade a bounded wait for an unbounded one.
    fn all_drained(&self) -> bool {
        self.0
            .lock()
            .is_ok_and(|conns| conns.iter().all(|c| c.upgrade().is_none()))
    }
}

/// Wait out one abandoned leg's upstream connections, metering and warning if any
/// is still held at the ceiling.
///
/// Called only on the cancel and `Err` paths — a clean `Ok` closes cooperatively
/// inside `drive` and needs no wait. A connection still held here is one this node
/// is about to strand, which is what makes an endpoint close hang, so it is worth a
/// counter and an operator-visible line rather than a silent return.
pub(super) async fn drain_abandoned(
    conns: &ConnDrain,
    provider_addr: Address,
    metrics: &crate::metrics::Metrics,
) {
    if conns.wait_drained(ABANDON_DRAIN_CAP).await {
        return;
    }
    metrics.node_pull_abandon_drain_timeout();
    tracing::warn!(
        provider = %provider_addr,
        "abandoned upstream did not drain within the cap; endpoint close may block"
    );
}

/// A [`PeerSource`] that records every connection it opens into a [`ConnDrain`].
///
/// The wrapper lives here rather than inside `PeerSource` because the hazard is
/// a property of THIS crate's per-serve runtimes: the publisher CLI drives the
/// same source from one long-lived runtime and can never strand a driver.
#[derive(Debug)]
pub(super) struct ObservedPeerSource<'a> {
    inner: PeerSource<'a>,
    conns: ConnDrain,
}

impl<'a> ObservedPeerSource<'a> {
    pub(super) fn new(inner: PeerSource<'a>) -> Self {
        Self {
            inner,
            conns: ConnDrain::default(),
        }
    }

    /// The record of connections opened so far, to wait on once the leg returns.
    pub(super) fn drain(&self) -> ConnDrain {
        self.conns.clone()
    }
}

impl BlobSource for ObservedPeerSource<'_> {
    type Reader = PullReader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        Box::pin(async move {
            let (header, reader) = self.inner.open(hash, range).await?;
            // Record EVERY gap's connection, not just the last: `drive` opens one
            // per gap, and an error path can leave more than one un-drained. A
            // handle whose connection already died costs nothing to hold.
            self.conns.record(reader.connection_handle());
            Ok((header, reader))
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        self.inner.finish(reader)
    }
}
