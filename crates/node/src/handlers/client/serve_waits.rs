//! Where one serve loop's time goes (#2348). A `serve_stream` span's
//! `idle_ns` counts all its unentered time as one total: the request read
//! and the handshake before the serve loop, and every await inside it.
//! [`ServeWaits`] times the serve loop's waits by cause, and records the QUIC
//! path's state as the loop ends, so a slow stream says which side holds it
//! back. The wait totals need not sum to `idle_ns`:
//!
//! - `store_wait_ns`: waiting for the next frame's bytes. On a hit that is the
//!   store's read; on a miss it includes the upstream pull that fills it.
//! - `send_wait_ns`: waiting to write a frame to the QUIC send stream. That
//!   wait grows when the path's congestion window, the connection's send
//!   window, or the client's flow-control window is full.
//! - `proof_wait_ns`: waiting to read and commit the client's proofs for the
//!   bytes on the wire. It grows when the client pays slowly, when the
//!   path's round trip is long, and while other streams of the same lane hold
//!   its lock.
//!
//! The path fields help to tell the send-side limits apart. A small
//! `path_cwnd` with `conn_lost_packets` points at the path. A large
//! `path_cwnd` with no loss points at the client's receive window: a client
//! that reads slowly, or a window smaller than the path's bandwidth-delay
//! product. `path_cwnd`, `path_rtt_us` and `path_kind` describe the selected
//! path as the loop ends; `path_congestion_events` is that path's lifetime
//! total. The `conn_*` counts are connection-wide deltas over the serve loop,
//! so streams that share the connection share them.

use std::future::Future;
use std::time::Duration;

use iroh::endpoint::{Connection, ConnectionStats};
use tokio::time::Instant;

use super::proof_wait::selected_path;

/// One wait a serve loop times ([`ServeWaits::time`]).
#[derive(Clone, Copy, Debug)]
pub(super) enum Wait {
    /// The next frame's bytes: the store's read, or the pull that fills it.
    Store,
    /// A write to the QUIC send stream.
    Send,
    /// Reading and committing the client's proofs.
    Proof,
}

/// The wait totals of one serve loop and the connection's counts at its
/// start. On drop it records the totals and the path's state on the
/// `serve_stream` span current at its construction, so every exit of the
/// serve loop records them: an end, an error, a stop or a cancel. A wait in
/// progress at a cancel counts up to the drop.
pub(super) struct ServeWaits {
    /// The `serve_stream` span, captured at construction: a cancelled task
    /// drops this guard outside the span.
    span: tracing::Span,
    /// The stream's connection, read again at the end.
    conn: Connection,
    /// The connection's counts as the serve loop starts.
    start: ConnCounts,
    /// The wait in progress and when it began: a cancel drops the guard
    /// with it still open, and the drop counts it.
    open: Option<(Wait, Instant)>,
    /// Time spent waiting for frame bytes.
    store: Duration,
    /// Time spent waiting on QUIC writes.
    send: Duration,
    /// Time spent waiting on the client's proofs.
    proof: Duration,
}

impl ServeWaits {
    /// Start timing a serve loop on `conn`, under the current span, which
    /// must be the `serve_stream` span that declares the fields.
    pub(super) fn start(conn: &Connection) -> Self {
        Self {
            span: tracing::Span::current(),
            conn: conn.clone(),
            start: ConnCounts::of(&conn.stats()),
            open: None,
            store: Duration::ZERO,
            send: Duration::ZERO,
            proof: Duration::ZERO,
        }
    }

    /// Await `fut` and add the time it took to `wait`'s total.
    pub(super) async fn time<T>(&mut self, wait: Wait, fut: impl Future<Output = T>) -> T {
        self.begin(wait);
        let out = fut.await;
        self.end();
        out
    }

    /// Begin a wait of `wait`'s kind, for an await too large to nest in
    /// [`ServeWaits::time`]. [`ServeWaits::end`] ends it.
    pub(super) fn begin(&mut self, wait: Wait) {
        self.end();
        self.open = Some((wait, Instant::now()));
    }

    /// End the wait in progress, if any, and add its time to its total.
    pub(super) fn end(&mut self) {
        let Some((wait, started)) = self.open.take() else {
            return;
        };
        let total = match wait {
            Wait::Store => &mut self.store,
            Wait::Send => &mut self.send,
            Wait::Proof => &mut self.proof,
        };
        *total = total.saturating_add(started.elapsed());
    }
}

/// The connection counts a [`ServeWaits`] records as deltas. Only these three
/// are kept, not the whole [`ConnectionStats`], which would grow every serve
/// future by its frame tables.
#[derive(Clone, Copy, Debug)]
struct ConnCounts {
    /// Packets lost on the connection.
    lost_packets: u64,
    /// Bytes lost on the connection.
    lost_bytes: u64,
    /// UDP bytes sent on the connection.
    sent_bytes: u64,
}

impl ConnCounts {
    /// The counts in `stats`.
    const fn of(stats: &ConnectionStats) -> Self {
        Self {
            lost_packets: stats.lost_packets,
            lost_bytes: stats.lost_bytes,
            sent_bytes: stats.udp_tx.bytes,
        }
    }
}

/// `d` in whole nanoseconds, saturating at `u64::MAX` (about 584 years).
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

impl Drop for ServeWaits {
    fn drop(&mut self) {
        self.end();
        let span = &self.span;
        span.record("store_wait_ns", nanos(self.store));
        span.record("send_wait_ns", nanos(self.send));
        span.record("proof_wait_ns", nanos(self.proof));
        let end = ConnCounts::of(&self.conn.stats());
        let start = self.start;
        span.record(
            "conn_lost_packets",
            end.lost_packets.saturating_sub(start.lost_packets),
        );
        span.record(
            "conn_lost_bytes",
            end.lost_bytes.saturating_sub(start.lost_bytes),
        );
        span.record(
            "conn_sent_bytes",
            end.sent_bytes.saturating_sub(start.sent_bytes),
        );
        let (_, selected) = selected_path(&self.conn);
        match selected {
            Some((kind, stats)) => {
                span.record("path_kind", kind);
                span.record(
                    "path_rtt_us",
                    u64::try_from(stats.rtt.as_micros()).unwrap_or(u64::MAX),
                );
                span.record("path_cwnd", stats.cwnd);
                span.record("path_congestion_events", stats.congestion_events);
            }
            None => {
                span.record("path_kind", "none");
            }
        }
    }
}
