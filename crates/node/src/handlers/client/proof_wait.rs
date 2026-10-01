//! The proof wait: how long the serve loop waits for the payer's next proof.
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
//! STREAM frame, and in every case when [`PROOF_WAIT_CEILING`] passes.
//! Keep-alive PINGs are not STREAM frames, so a connection that only keeps
//! itself alive shows no progress.
//!
//! The count is per connection. Sibling streams on the same connection, and
//! retransmissions on a lossy path, also advance it. The ceiling bounds the
//! wait in those cases.
//!
//! Both faults carry [`PeerFault`](super::wire::PeerFault) and name the
//! connection's selected path and its congestion state, so the `serve_stream`
//! failure span says which path stalled.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use iroh::TransportAddr;
use iroh::endpoint::Connection;
use tokio::time::{Instant, MissedTickBehavior};

use super::VOUCHER_READ_TIMEOUT;

/// The longest the serve loop waits for one proof, transport progress or not.
/// The clock starts when the wait for that proof starts.
pub(super) const PROOF_WAIT_CEILING: Duration = Duration::from_secs(30);

/// How often a proof wait samples the connection's STREAM frame count. It
/// sets the resolution of the no-progress fault: the wait faults at most one
/// poll after [`VOUCHER_READ_TIMEOUT`] of no progress.
const PROGRESS_POLL: Duration = Duration::from_secs(1);

/// What a proof wait does at one progress sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Keep waiting for the proof.
    Wait,
    /// No proof and no new STREAM frame for [`VOUCHER_READ_TIMEOUT`].
    Stalled,
    /// The wait for this proof passed [`PROOF_WAIT_CEILING`].
    PastCeiling,
}

/// Decide a proof wait from how long it has run (`waited`) and how long ago
/// the connection last sent a new STREAM frame (`since_progress`).
fn proof_wait_verdict(waited: Duration, since_progress: Duration) -> Verdict {
    if since_progress >= VOUCHER_READ_TIMEOUT {
        Verdict::Stalled
    } else if waited >= PROOF_WAIT_CEILING {
        Verdict::PastCeiling
    } else {
        Verdict::Wait
    }
}

/// The progress state of one proof wait.
struct ProgressClock {
    /// When the wait for this proof started.
    started: Instant,
    /// The STREAM frame count at the last sample that saw it change.
    frames: u64,
    /// When a sample last saw the STREAM frame count change, or `started`.
    progressed_at: Instant,
}

impl ProgressClock {
    /// Start a wait at `now`, with `frames` STREAM frames already sent.
    const fn new(frames: u64, now: Instant) -> Self {
        Self {
            started: now,
            frames,
            progressed_at: now,
        }
    }

    /// Record a sample of `frames` taken at `now`, and decide the wait.
    fn observe(&mut self, frames: u64, now: Instant) -> Verdict {
        if frames != self.frames {
            self.frames = frames;
            self.progressed_at = now;
        }
        proof_wait_verdict(
            now.saturating_duration_since(self.started),
            now.saturating_duration_since(self.progressed_at),
        )
    }
}

/// Wait for `read`, the next proof, on `conn`. See the module docs for when
/// the wait faults.
///
/// # Errors
///
/// The error of `read`, or a [`PeerFault`](super::wire::PeerFault) when the
/// wait stalls or passes its ceiling.
pub(super) async fn await_proof<T>(
    read: impl Future<Output = anyhow::Result<T>>,
    conn: &Connection,
) -> anyhow::Result<T> {
    wait_for_proof(read, || stream_frames_sent(conn), || describe_path(conn)).await
}

/// The body of [`await_proof`], with the connection reduced to its STREAM
/// frame count (`stream_frames`) and its path report (`path`).
async fn wait_for_proof<T>(
    read: impl Future<Output = anyhow::Result<T>>,
    stream_frames: impl Fn() -> u64,
    path: impl Fn() -> String,
) -> anyhow::Result<T> {
    let start = Instant::now();
    let mut clock = ProgressClock::new(stream_frames(), start);
    let mut poll = tokio::time::interval_at(start + PROGRESS_POLL, PROGRESS_POLL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // One read future for the whole wait: a sample does not cancel the read.
    let mut read = std::pin::pin!(read);
    loop {
        tokio::select! {
            biased;
            proof = &mut read => return proof,
            _ = poll.tick() => {
                let reason = match clock.observe(stream_frames(), Instant::now()) {
                    Verdict::Wait => continue,
                    Verdict::Stalled => format!(
                        "no proof and no transport progress for {VOUCHER_READ_TIMEOUT:?}"
                    ),
                    Verdict::PastCeiling => {
                        format!("proof wait passed {PROOF_WAIT_CEILING:?}")
                    }
                };
                return Err(anyhow::Error::new(super::wire::PeerFault)
                    .context(format!("{reason} ({})", path())));
            }
        }
    }
}

/// The count of STREAM frames `conn` has sent, over all its paths.
fn stream_frames_sent(conn: &Connection) -> u64 {
    conn.stats().frame_tx.stream
}

/// The selected path of `conn` and its congestion state, for a proof-wait
/// fault: relay or direct, IPv4 or IPv6, then RTT, congestion window, lost
/// packets, congestion events and black holes detected. The remote address
/// itself stays out of the report.
fn describe_path(conn: &Connection) -> String {
    let paths = conn.paths();
    let open = paths.len();
    let Some(path) = paths.iter().find(iroh::endpoint::Path::is_selected) else {
        return format!("no selected path, {open} open");
    };
    let kind = match path.remote_addr() {
        TransportAddr::Relay(_) => "relay",
        TransportAddr::Ip(SocketAddr::V4(_)) => "direct IPv4",
        TransportAddr::Ip(SocketAddr::V6(v6)) if v6.ip().to_canonical().is_ipv4() => "direct IPv4",
        TransportAddr::Ip(SocketAddr::V6(_)) => "direct IPv6",
        _ => "custom transport",
    };
    let stats = path.stats();
    format!(
        "selected path {kind} of {open} open: rtt {:?}, cwnd {} bytes, {} packets lost, \
         {} congestion events, {} black holes detected",
        stats.rtt,
        stats.cwnd,
        stats.lost_packets,
        stats.congestion_events,
        stats.black_holes_detected
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn a_wait_with_progress_keeps_waiting() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        // Each sample sees new STREAM frames, so the no-progress clock restarts
        // and the wait runs past the plain timeout.
        for (i, frames) in (1..=25u32).zip(6u64..) {
            assert_eq!(
                clock.observe(frames, start + SECOND * i),
                Verdict::Wait,
                "a wait that sees progress at {i}s keeps waiting"
            );
        }
    }

    #[test]
    fn a_wait_with_no_progress_faults_at_the_timeout() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        assert_eq!(
            clock.observe(5, start + VOUCHER_READ_TIMEOUT - SECOND),
            Verdict::Wait
        );
        assert_eq!(
            clock.observe(5, start + VOUCHER_READ_TIMEOUT),
            Verdict::Stalled
        );
    }

    #[test]
    fn the_no_progress_clock_runs_from_the_last_progress() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        // The buffered bytes drain for 4s, then the connection goes quiet.
        assert_eq!(clock.observe(9, start + SECOND * 4), Verdict::Wait);
        assert_eq!(clock.observe(9, start + SECOND * 13), Verdict::Wait);
        assert_eq!(clock.observe(9, start + SECOND * 14), Verdict::Stalled);
    }

    #[test]
    fn a_wait_past_the_ceiling_faults_even_with_progress() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        assert_eq!(
            clock.observe(6, start + PROOF_WAIT_CEILING - SECOND),
            Verdict::Wait
        );
        assert_eq!(
            clock.observe(7, start + PROOF_WAIT_CEILING),
            Verdict::PastCeiling
        );
    }

    #[test]
    fn the_verdict_bounds() {
        assert_eq!(proof_wait_verdict(SECOND, SECOND), Verdict::Wait);
        assert_eq!(
            proof_wait_verdict(VOUCHER_READ_TIMEOUT, VOUCHER_READ_TIMEOUT),
            Verdict::Stalled
        );
        assert_eq!(
            proof_wait_verdict(PROOF_WAIT_CEILING, SECOND),
            Verdict::PastCeiling
        );
        // No progress at the ceiling names the stall, the more specific cause.
        assert_eq!(
            proof_wait_verdict(PROOF_WAIT_CEILING, PROOF_WAIT_CEILING),
            Verdict::Stalled
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_proof_that_arrives_ends_the_wait() -> anyhow::Result<()> {
        let read = async {
            tokio::time::sleep(SECOND * 3).await;
            Ok(7u32)
        };
        let got = wait_for_proof(read, || 0, String::new).await?;
        anyhow::ensure!(got == 7, "the proof passes through, got {got}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_connection_faults_at_the_timeout() -> anyhow::Result<()> {
        let start = Instant::now();
        let read = std::future::pending::<anyhow::Result<()>>();
        let Err(e) = wait_for_proof(read, || 0, || "a test path".to_owned()).await else {
            anyhow::bail!("a wait with no proof must fault");
        };
        let waited = start.elapsed();
        anyhow::ensure!(
            waited == VOUCHER_READ_TIMEOUT,
            "a quiet connection faults at the timeout, waited {waited:?}"
        );
        anyhow::ensure!(e.is::<super::super::wire::PeerFault>(), "{e:#}");
        let text = format!("{e:#}");
        anyhow::ensure!(
            text.contains("no transport progress for 10s") && text.contains("a test path"),
            "{text}"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_draining_connection_waits_to_the_ceiling() -> anyhow::Result<()> {
        let start = Instant::now();
        let frames = Arc::new(AtomicU64::new(0));
        let sent = Arc::clone(&frames);
        // Every sample sees one more STREAM frame: the send buffer never drains.
        let count = move || sent.fetch_add(1, Ordering::Relaxed);
        let read = std::future::pending::<anyhow::Result<()>>();
        let Err(e) = wait_for_proof(read, count, String::new).await else {
            anyhow::bail!("a wait with no proof must fault");
        };
        let waited = start.elapsed();
        anyhow::ensure!(
            waited == PROOF_WAIT_CEILING,
            "a draining connection faults at the ceiling, waited {waited:?}"
        );
        anyhow::ensure!(e.is::<super::super::wire::PeerFault>(), "{e:#}");
        anyhow::ensure!(format!("{e:#}").contains("proof wait passed 30s"), "{e:#}");
        Ok(())
    }
}
