//! The proof wait: how long the serve loop waits for the payer's proofs of one
//! owed chunk.
//!
//! A write returns once the transport has buffered its bytes, not once the
//! client has them. The node's send buffer can hold several chunks, so the
//! bytes a proof answers can still be in flight when the wait starts. On a slow
//! or young path, a fixed clock from that point faults a paying client whose
//! bytes have not reached it yet (#2230).
//!
//! So the wait counts transport progress as well as time. Every
//! [`PROGRESS_POLL`] it samples how many STREAM frames the connection has sent.
//! It faults when [`VOUCHER_READ_TIMEOUT`] passes with no proof and no new
//! STREAM frame. Keep-alive PINGs are not STREAM frames, so a connection that
//! only keeps itself alive shows no progress.
//!
//! The count is per connection. Sibling streams on the same connection, and
//! retransmissions on a lossy path, also advance it. So a payer that stops
//! reading while the connection carries other traffic can hold its wait until
//! the ceiling.
//!
//! The ceiling is per owed chunk, not per proof. A chunk can take up to
//! `MAX_PROOFS_PER_CHUNK` proofs, and [`ChunkProofs`] counts them and holds
//! one deadline for all of them: [`PROOF_WAIT_CEILING`] after the chunk's
//! first wait starts. The deadline fires on time, whatever the progress
//! samples show, and a wait that starts after it faults at once unless its
//! proof is already buffered. So proofs that credit nothing cannot hold one
//! owed chunk past the ceiling.
//!
//! Both faults carry [`PeerFault`](super::wire::PeerFault) and a
//! [`ProofWaitFault`] that names its kind. The fault text gives the measured
//! times, the STREAM frames the wait saw, and the connection's selected path
//! with its congestion state, so the `serve_stream` failure span says which
//! path stalled.

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use iroh::TransportAddr;
use iroh::endpoint::{Connection, PathStats};
use tokio::time::{Instant, MissedTickBehavior};

use super::{MAX_PROOFS_PER_CHUNK, VOUCHER_READ_TIMEOUT};

/// The longest the serve loop waits for the proofs of one owed chunk,
/// transport progress or not. The clock starts with the chunk's first proof
/// wait and runs across all of its proofs ([`ChunkProofs`]).
pub(crate) const PROOF_WAIT_CEILING: Duration = Duration::from_secs(30);

/// How often a proof wait samples the connection's STREAM frame count. It
/// sets the resolution of the no-progress fault: the wait faults at most one
/// poll after [`VOUCHER_READ_TIMEOUT`] of no progress.
const PROGRESS_POLL: Duration = Duration::from_secs(1);

/// The proofs the serve loop reads for one owed chunk: how many it has read,
/// and when the chunk's proof wait started. One value per owed chunk, made
/// before the chunk's first proof, so the proof budget and the ceiling both
/// span every proof for that chunk.
#[derive(Debug)]
pub(super) struct ChunkProofs {
    /// Proofs read for the chunk so far, the one being read included.
    attempts: u32,
    /// When the chunk's first proof wait started.
    started: Instant,
}

impl ChunkProofs {
    /// Start the proofs of a chunk whose first proof wait starts now.
    pub(super) fn start() -> Self {
        Self {
            attempts: 0,
            started: Instant::now(),
        }
    }

    /// Count one more proof for the chunk, and return its 1-based number.
    pub(super) const fn next_attempt(&mut self) -> u32 {
        self.attempts = self.attempts.saturating_add(1);
        self.attempts
    }

    /// The proofs read for the chunk so far.
    pub(super) const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Whether the chunk has used its whole proof budget,
    /// `MAX_PROOFS_PER_CHUNK`.
    pub(super) const fn exhausted(&self) -> bool {
        self.attempts >= MAX_PROOFS_PER_CHUNK
    }

    /// When the chunk's proof wait passes [`PROOF_WAIT_CEILING`].
    fn expires_at(&self) -> Instant {
        self.started + PROOF_WAIT_CEILING
    }
}

/// Which limit a proof wait hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProofWaitKind {
    /// No proof and no new STREAM frame for [`VOUCHER_READ_TIMEOUT`].
    Stalled,
    /// The chunk's proof wait passed [`PROOF_WAIT_CEILING`].
    PastCeiling,
}

/// The marker a proof-wait fault carries beside
/// [`PeerFault`](super::wire::PeerFault): which limit the wait hit, and the
/// measured detail. Its `Display` is the detail, so the error chain reads as
/// one line.
#[derive(Debug)]
pub(super) struct ProofWaitFault {
    /// Which limit the wait hit.
    kind: ProofWaitKind,
    /// The measured times, frames and path, as the error chain prints them.
    detail: String,
}

impl fmt::Display for ProofWaitFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl ProofWaitFault {
    /// Whether `e` ended at its chunk's proof-wait ceiling.
    pub(super) fn is_past_ceiling(e: &anyhow::Error) -> bool {
        e.downcast_ref::<Self>()
            .is_some_and(|f| f.kind == ProofWaitKind::PastCeiling)
    }

    /// A [`PeerFault`](super::wire::PeerFault) error that carries this marker.
    fn into_error(self) -> anyhow::Error {
        anyhow::Error::new(super::wire::PeerFault).context(self)
    }
}

/// Whether a proof wait has stalled: no new STREAM frame for
/// `since_progress`, which reaches [`VOUCHER_READ_TIMEOUT`].
fn is_stalled(since_progress: Duration) -> bool {
    since_progress >= VOUCHER_READ_TIMEOUT
}

/// The transport progress of one proof wait.
struct ProgressClock {
    /// The STREAM frame count when this wait started.
    frames_at_start: u64,
    /// The STREAM frame count at the last sample that saw it change.
    frames: u64,
    /// When a sample last saw the STREAM frame count change, or when this
    /// wait started.
    progressed_at: Instant,
}

impl ProgressClock {
    /// Start a wait at `now`, with `frames` STREAM frames already sent.
    const fn new(frames: u64, now: Instant) -> Self {
        Self {
            frames_at_start: frames,
            frames,
            progressed_at: now,
        }
    }

    /// Record a sample of `frames` taken at `now`, and return how long the
    /// connection has gone without a new STREAM frame.
    fn observe(&mut self, frames: u64, now: Instant) -> Duration {
        if frames != self.frames {
            self.frames = frames;
            self.progressed_at = now;
        }
        now.saturating_duration_since(self.progressed_at)
    }

    /// The STREAM frames the connection sent during this wait, up to the last
    /// sample.
    const fn frames_sent(&self) -> u64 {
        self.frames.saturating_sub(self.frames_at_start)
    }
}

/// Wait for `read`, the next proof of the chunk that `proofs` counts, on
/// `conn`. See the module docs for when the wait faults.
///
/// # Errors
///
/// The error of `read`, or a [`PeerFault`](super::wire::PeerFault) with a
/// [`ProofWaitFault`] when the wait stalls or the chunk passes its ceiling.
pub(super) async fn await_proof<T>(
    read: impl Future<Output = anyhow::Result<T>>,
    conn: &Connection,
    proofs: &ChunkProofs,
) -> anyhow::Result<T> {
    wait_for_proof(
        read,
        proofs,
        || stream_frames_sent(conn),
        || describe_path(conn),
    )
    .await
}

/// The body of [`await_proof`], with the connection reduced to its STREAM
/// frame count (`stream_frames`) and its path report (`path`).
async fn wait_for_proof<T>(
    read: impl Future<Output = anyhow::Result<T>>,
    proofs: &ChunkProofs,
    stream_frames: impl Fn() -> u64,
    path: impl Fn() -> String,
) -> anyhow::Result<T> {
    let start = Instant::now();
    let mut clock = ProgressClock::new(stream_frames(), start);
    let mut poll = tokio::time::interval_at(start + PROGRESS_POLL, PROGRESS_POLL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let ceiling = tokio::time::sleep_until(proofs.expires_at());
    // One read future for the whole wait: a sample does not cancel the read.
    let mut read = std::pin::pin!(read);
    let mut ceiling = std::pin::pin!(ceiling);
    // A proof that is ready wins over both limits. A progress sample comes
    // before the ceiling, so a stall that lands on the ceiling's instant is
    // reported as the stall, the more specific cause. The fault reports the
    // sample that decided it, so a frame sent after the decision cannot make
    // the report contradict it.
    let (kind, now, since_progress) = loop {
        tokio::select! {
            biased;
            proof = &mut read => return proof,
            _ = poll.tick() => {
                let now = Instant::now();
                let since_progress = clock.observe(stream_frames(), now);
                if is_stalled(since_progress) {
                    break (ProofWaitKind::Stalled, now, since_progress);
                }
            }
            () = &mut ceiling => {
                let now = Instant::now();
                let since_progress = clock.observe(stream_frames(), now);
                break (ProofWaitKind::PastCeiling, now, since_progress);
            }
        }
    };
    let waited = now.saturating_duration_since(proofs.started);
    let frames = clock.frames_sent();
    let detail = match kind {
        ProofWaitKind::Stalled => format!(
            "no proof and no transport progress for {since_progress:.1?}; the chunk has waited \
             {waited:.1?} for its proofs, and this proof wait sent {frames} STREAM frames ({})",
            path()
        ),
        ProofWaitKind::PastCeiling => format!(
            "the chunk's proofs ran past their wait ceiling after {waited:.1?}; no transport \
             progress for {since_progress:.1?}, and this proof wait sent {frames} STREAM frames \
             ({})",
            path()
        ),
    };
    Err(ProofWaitFault { kind, detail }.into_error())
}

/// The count of STREAM frames `conn` has sent, over all its paths.
fn stream_frames_sent(conn: &Connection) -> u64 {
    conn.stats().frame_tx.stream
}

/// The selected path of `conn` and its congestion state, for a proof-wait
/// fault: relay or direct, IPv4 or IPv6, the current RTT and congestion
/// window, then the path's lifetime totals of lost packets, congestion events
/// and black holes detected. The remote address itself stays out of the
/// report.
fn describe_path(conn: &Connection) -> String {
    let (open, selected) = selected_path(conn);
    let Some((kind, stats)) = selected else {
        return format!("no selected path, {open} open");
    };
    format!(
        "selected path {kind} of {open} open: rtt {:?}, cwnd {} bytes; path lifetime totals: \
         {} packets lost, {} congestion events, {} black holes detected",
        stats.rtt,
        stats.cwnd,
        stats.lost_packets,
        stats.congestion_events,
        stats.black_holes_detected
    )
}

/// The count of open paths of `conn`, and its selected path's kind and
/// stats: relay, direct IPv4, direct IPv6, or a custom transport. The remote
/// address itself stays out.
/// `None` for the selected path when no path is selected.
pub(super) fn selected_path(conn: &Connection) -> (usize, Option<(&'static str, PathStats)>) {
    let paths = conn.paths();
    let open = paths.len();
    let Some(path) = paths.iter().find(iroh::endpoint::Path::is_selected) else {
        return (open, None);
    };
    let kind = match path.remote_addr() {
        TransportAddr::Relay(_) => "relay",
        TransportAddr::Ip(SocketAddr::V4(_)) => "direct IPv4",
        TransportAddr::Ip(SocketAddr::V6(v6)) if v6.ip().to_canonical().is_ipv4() => "direct IPv4",
        TransportAddr::Ip(SocketAddr::V6(_)) => "direct IPv6",
        _ => "custom transport",
    };
    (open, Some((kind, path.stats())))
}

#[cfg(test)]
mod tests;
