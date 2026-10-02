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
use iroh::endpoint::Connection;
use tokio::time::{Instant, MissedTickBehavior};

use super::{MAX_PROOFS_PER_CHUNK, VOUCHER_READ_TIMEOUT};

/// The longest the serve loop waits for the proofs of one owed chunk,
/// transport progress or not. The clock starts with the chunk's first proof
/// wait and runs across all of its proofs ([`ChunkProofs`]).
pub(super) const PROOF_WAIT_CEILING: Duration = Duration::from_secs(30);

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
        "selected path {kind} of {open} open: rtt {:?}, cwnd {} bytes; path lifetime totals: \
         {} packets lost, {} congestion events, {} black holes detected",
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

    use iroh::Endpoint;
    use iroh::endpoint::{QuicTransportConfig, presets};

    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// A chunk's proofs whose first wait started at `at`.
    const fn proofs_from(at: Instant) -> ChunkProofs {
        ChunkProofs {
            attempts: 0,
            started: at,
        }
    }

    /// A STREAM frame count that rises by one at every sample: a connection
    /// whose frames keep going out.
    fn rising() -> impl Fn() -> u64 {
        let frames = Arc::new(AtomicU64::new(0));
        move || frames.fetch_add(1, Ordering::Relaxed)
    }

    #[test]
    fn a_wait_with_progress_keeps_waiting() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        // Each sample sees new STREAM frames, so the no-progress clock restarts
        // and the wait runs past the plain timeout.
        for (i, frames) in (1..=25u32).zip(6u64..) {
            let since = clock.observe(frames, start + SECOND * i);
            assert!(
                !is_stalled(since),
                "a wait that sees progress at {i}s keeps waiting"
            );
        }
        assert_eq!(clock.frames_sent(), 25);
    }

    #[test]
    fn a_wait_with_no_progress_stalls_at_the_timeout() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        assert!(!is_stalled(clock.observe(
            5,
            start + VOUCHER_READ_TIMEOUT.saturating_sub(SECOND)
        )));
        assert!(is_stalled(clock.observe(5, start + VOUCHER_READ_TIMEOUT)));
        assert_eq!(clock.frames_sent(), 0);
    }

    #[test]
    fn the_no_progress_clock_runs_from_the_last_progress() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(5, start);
        // The buffered bytes drain for 4s, then the connection goes quiet.
        assert!(!is_stalled(clock.observe(9, start + SECOND * 4)));
        assert!(!is_stalled(clock.observe(9, start + SECOND * 13)));
        assert!(is_stalled(clock.observe(9, start + SECOND * 14)));
    }

    #[test]
    fn a_chunks_proofs_count_against_one_budget_and_one_deadline() {
        let start = Instant::now();
        let mut proofs = proofs_from(start);
        for n in 1..MAX_PROOFS_PER_CHUNK {
            assert_eq!(proofs.next_attempt(), n);
            assert!(!proofs.exhausted(), "{n} proofs leave budget");
        }
        assert_eq!(proofs.next_attempt(), MAX_PROOFS_PER_CHUNK);
        assert!(proofs.exhausted());
        assert_eq!(proofs.attempts(), MAX_PROOFS_PER_CHUNK);
        assert_eq!(proofs.expires_at(), start + PROOF_WAIT_CEILING);
    }

    #[tokio::test(start_paused = true)]
    async fn a_proof_that_arrives_ends_the_wait() -> anyhow::Result<()> {
        let read = async {
            tokio::time::sleep(SECOND * 3).await;
            Ok(7u32)
        };
        let got = wait_for_proof(read, &ChunkProofs::start(), || 0, String::new).await?;
        anyhow::ensure!(got == 7, "the proof passes through, got {got}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_connection_faults_at_the_timeout() -> anyhow::Result<()> {
        let start = Instant::now();
        let read = std::future::pending::<anyhow::Result<()>>();
        let proofs = ChunkProofs::start();
        let Err(e) = wait_for_proof(read, &proofs, || 0, || "a test path".to_owned()).await else {
            anyhow::bail!("a wait with no proof must fault");
        };
        let waited = start.elapsed();
        anyhow::ensure!(
            waited == VOUCHER_READ_TIMEOUT,
            "a quiet connection faults at the timeout, waited {waited:?}"
        );
        anyhow::ensure!(e.is::<super::super::wire::PeerFault>(), "{e:#}");
        anyhow::ensure!(!ProofWaitFault::is_past_ceiling(&e), "{e:#}");
        anyhow::ensure!(
            e.downcast_ref::<ProofWaitFault>()
                .is_some_and(|f| f.kind == ProofWaitKind::Stalled),
            "{e:#}"
        );
        let text = format!("{e:#}");
        anyhow::ensure!(
            text.contains("no transport progress for 10.0s")
                && text.contains("this proof wait sent 0 STREAM frames")
                && text.contains("a test path"),
            "{text}"
        );
        Ok(())
    }

    /// A frame sent after the stall is decided does not change the report: it
    /// gives the sample that decided the stall.
    #[tokio::test(start_paused = true)]
    async fn a_stall_reports_the_sample_that_decided_it() -> anyhow::Result<()> {
        // The start sample and the ten 1s samples see no frame. Any later
        // sample sees one.
        let samples = Arc::new(AtomicU64::new(0));
        let frames = {
            let samples = Arc::clone(&samples);
            move || u64::from(samples.fetch_add(1, Ordering::Relaxed) > 10)
        };
        let read = std::future::pending::<anyhow::Result<()>>();
        let Err(e) = wait_for_proof(read, &ChunkProofs::start(), frames, String::new).await else {
            anyhow::bail!("a wait with no proof must fault");
        };
        anyhow::ensure!(
            e.downcast_ref::<ProofWaitFault>()
                .is_some_and(|f| f.kind == ProofWaitKind::Stalled),
            "{e:#}"
        );
        let text = format!("{e:#}");
        anyhow::ensure!(
            text.contains("no transport progress for 10.0s")
                && text.contains("this proof wait sent 0 STREAM frames"),
            "{text}"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_draining_connection_waits_to_the_ceiling() -> anyhow::Result<()> {
        let start = Instant::now();
        let read = std::future::pending::<anyhow::Result<()>>();
        let proofs = ChunkProofs::start();
        let Err(e) = wait_for_proof(read, &proofs, rising(), String::new).await else {
            anyhow::bail!("a wait with no proof must fault");
        };
        let waited = start.elapsed();
        anyhow::ensure!(
            waited == PROOF_WAIT_CEILING,
            "a draining connection faults at the ceiling, waited {waited:?}"
        );
        anyhow::ensure!(e.is::<super::super::wire::PeerFault>(), "{e:#}");
        anyhow::ensure!(ProofWaitFault::is_past_ceiling(&e), "{e:#}");
        anyhow::ensure!(
            format!("{e:#}").contains("ran past their wait ceiling after 30.0s"),
            "{e:#}"
        );
        Ok(())
    }

    /// Read proofs for one chunk the way the recoup loops do: proof `n` lands
    /// `gap(n)` after its wait starts, and credits nothing. Return the fault,
    /// the proofs read before it, and the time the chunk held the stream.
    async fn zero_credit_run(
        gap: impl Fn(u32) -> Duration,
    ) -> anyhow::Result<(anyhow::Error, u32, Duration)> {
        let start = Instant::now();
        let frames = rising();
        let mut proofs = ChunkProofs::start();
        loop {
            let n = proofs.next_attempt();
            let read = async {
                tokio::time::sleep(gap(n)).await;
                Ok(())
            };
            if let Err(e) = wait_for_proof(read, &proofs, &frames, String::new).await {
                return Ok((e, proofs.attempts().saturating_sub(1), start.elapsed()));
            }
            anyhow::ensure!(
                !proofs.exhausted(),
                "the chunk took {} proofs without a wait fault",
                proofs.attempts()
            );
        }
    }

    /// A payer that sends a proof which credits nothing just before each
    /// per-proof deadline, on a connection whose frames keep advancing (a
    /// sibling stream still receives), cannot hold one chunk past the ceiling:
    /// every proof wait counts it from the chunk's first wait.
    #[tokio::test(start_paused = true)]
    async fn repeated_zero_credit_proofs_cannot_extend_a_chunk_past_the_ceiling()
    -> anyhow::Result<()> {
        let (fault, proofs, held) =
            zero_credit_run(|_| PROOF_WAIT_CEILING.saturating_sub(SECOND)).await?;
        anyhow::ensure!(
            held == PROOF_WAIT_CEILING,
            "zero-credit proofs held the chunk for {held:?}"
        );
        anyhow::ensure!(
            proofs == 1,
            "the chunk took {proofs} proofs before its fault"
        );
        anyhow::ensure!(ProofWaitFault::is_past_ceiling(&fault), "{fault:#}");
        Ok(())
    }

    /// Proofs that land faster than a progress sample, around the chunk's
    /// deadline, cannot extend it: the ceiling fires on time, and a wait that
    /// starts after it faults at once.
    #[tokio::test(start_paused = true)]
    async fn sub_second_proofs_after_the_deadline_cannot_extend_a_chunk() -> anyhow::Result<()> {
        // The first proof lands 0.5s before the deadline. Every later one lands
        // 0.7s into its wait, before that wait's first progress sample at 1s.
        let gap = Duration::from_millis(700);
        let (fault, proofs, held) = zero_credit_run(|n| {
            if n == 1 {
                PROOF_WAIT_CEILING.saturating_sub(Duration::from_millis(500))
            } else {
                gap
            }
        })
        .await?;
        anyhow::ensure!(
            held == PROOF_WAIT_CEILING,
            "sub-second proofs held the chunk for {held:?}"
        );
        anyhow::ensure!(
            proofs == 1,
            "the chunk took {proofs} proofs before its fault"
        );
        anyhow::ensure!(ProofWaitFault::is_past_ceiling(&fault), "{fault:#}");

        // A wait that starts after the deadline faults at once.
        let late = proofs_from(Instant::now());
        tokio::time::advance(PROOF_WAIT_CEILING + SECOND).await;
        let started = Instant::now();
        let read = async {
            tokio::time::sleep(gap).await;
            Ok(())
        };
        let Err(e) = wait_for_proof(read, &late, rising(), String::new).await else {
            anyhow::bail!("a wait that starts past the ceiling must fault");
        };
        anyhow::ensure!(started.elapsed() == Duration::ZERO, "{e:#}");
        anyhow::ensure!(ProofWaitFault::is_past_ceiling(&e), "{e:#}");

        // A proof already buffered still wins.
        let ready = wait_for_proof(async { Ok(3u8) }, &late, rising(), String::new).await?;
        anyhow::ensure!(ready == 3, "a buffered proof passes through, got {ready}");
        Ok(())
    }

    /// The seam the wait relies on: on a real connection, writing stream data
    /// raises the STREAM frame count, and keep-alive PINGs on an idle
    /// connection leave it flat.
    #[tokio::test(flavor = "multi_thread")]
    async fn stream_frames_count_stream_data_and_not_keep_alives() -> anyhow::Result<()> {
        const ALPN: &[u8] = b"decdn/test/proof-wait-seam";
        let transport = || {
            QuicTransportConfig::builder()
                .keep_alive_interval(Duration::from_millis(200))
                .build()
        };
        let bind = std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0);
        let server = Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .transport_config(transport())
            .bind_addr(bind)?
            .bind()
            .await?;
        let client = Endpoint::builder(presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .transport_config(transport())
            .bind_addr(bind)?
            .bind()
            .await?;
        let socket = server
            .bound_sockets()
            .into_iter()
            .find(SocketAddr::is_ipv4)
            .ok_or_else(|| anyhow::anyhow!("no IPv4 bound socket"))?;
        let target = iroh::EndpointAddr::new(server.id()).with_ip_addr(socket);

        let accept = {
            let server = server.clone();
            tokio::spawn(async move {
                let incoming = server
                    .accept()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("server closed"))?;
                let conn = incoming.await?;
                let (_send, mut recv) = conn.accept_bi().await?;
                let got = recv.read_to_end(64 * 1024).await?;
                anyhow::Ok((conn, got.len()))
            })
        };
        let conn = client.connect(target, ALPN).await?;
        let before = stream_frames_sent(&conn);
        let (mut send, _recv) = conn.open_bi().await?;
        send.write_all(&[0x5Au8; 8 * 1024]).await?;
        send.finish()?;
        let (server_conn, got) = accept.await??;
        anyhow::ensure!(got == 8 * 1024, "the server read {got} bytes");
        let after_write = stream_frames_sent(&conn);
        anyhow::ensure!(
            after_write > before,
            "writing stream data must raise the STREAM frame count: {before} -> {after_write}"
        );

        // Idle with keep-alive on: PINGs go out, STREAM frames do not.
        let pings = conn.stats().frame_tx.ping;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let stats = conn.stats();
        anyhow::ensure!(
            stats.frame_tx.ping > pings,
            "keep-alive sent no PING in 3s: {pings} -> {}",
            stats.frame_tx.ping
        );
        anyhow::ensure!(
            stats.frame_tx.stream == after_write,
            "an idle connection must send no STREAM frame: {after_write} -> {}",
            stats.frame_tx.stream
        );
        anyhow::ensure!(
            describe_path(&conn).starts_with("selected path direct IPv4"),
            "{}",
            describe_path(&conn)
        );

        conn.close(0u32.into(), b"done");
        server_conn.close(0u32.into(), b"done");
        client.close().await;
        server.close().await;
        Ok(())
    }
}
