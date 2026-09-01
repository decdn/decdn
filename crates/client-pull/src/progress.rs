//! Sub-frame progress observation for the paid-pull receive path.
//!
//! `ProgressReader` wraps the QUIC receive stream as an `AsyncRead` and counts bytes as
//! they arrive, including mid-frame. `read_frame` fills a whole message with one
//! `read_exact`, and that call drives many `poll_read`s, each of which advances the
//! counter. The counter is the primitive the throughput floor reads: it is
//! frame-size-independent because it moves on bytes off the wire, not on decoded frames.

use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::Instant;

/// An `AsyncRead` that tallies every byte it yields into a shared counter.
///
/// Wrapping the receive stream at the byte layer — beneath `read_frame`'s message
/// decode — is what makes progress observable inside a single large frame. Each
/// `poll_read` adds the bytes it filled, so a frame that arrives in many transport
/// reads advances the counter continuously rather than all at once on decode.
pub(crate) struct ProgressReader<'a, R> {
    inner: &'a mut R,
    counter: Arc<AtomicU64>,
}

impl<'a, R> ProgressReader<'a, R> {
    /// Wrap `inner`, adding each read's byte count to `counter`.
    pub(crate) const fn new(inner: &'a mut R, counter: Arc<AtomicU64>) -> Self {
        Self { inner, counter }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut *self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            let delta = buf.filled().len().saturating_sub(before) as u64;
            if delta > 0 {
                self.counter.fetch_add(delta, Ordering::Relaxed);
            }
        }
        res
    }
}

/// The streaming-stage stall policy: a minimum throughput `floor_bps` sustained over a
/// trailing `window`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FloorConfig {
    /// The trailing window over which throughput is measured.
    pub window: Duration,
    /// The minimum bytes-per-second the transfer must sustain over the window. `0`
    /// disables the throughput test and leaves pure idle detection: at least one byte
    /// per window.
    pub floor_bps: u64,
}

/// The outcome of one [`ThroughputFloor::evaluate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FloorVerdict {
    /// Throughput is at or above the floor, or the window is not yet judgeable.
    Ok,
    /// Throughput has stayed below the floor across a full window.
    Stalled,
}

/// Watches a byte counter and reports a stall when throughput falls below the floor.
///
/// It reads the same [`ProgressReader`] counter the receive loop is filling, so the
/// signal is frame-size-independent. The window must warm — a full `window` of streaming
/// must elapse — before the floor can fire, which gives the first-byte stage its own time
/// budget: for the first window no byte is required to have arrived. [`Self::pause`] /
/// [`Self::resume`] exclude a self-inflicted credit-window payment pause, so the payer's
/// own unpaid balance is never read as a sender stall.
pub(crate) struct ThroughputFloor {
    cfg: FloorConfig,
    counter: Arc<AtomicU64>,
    samples: VecDeque<(Instant, u64)>,
    started: Instant,
    paused_since: Option<Instant>,
}

impl ThroughputFloor {
    /// Start watching `counter` from `started` (the moment streaming begins).
    pub(crate) const fn new(cfg: FloorConfig, counter: Arc<AtomicU64>, started: Instant) -> Self {
        Self {
            cfg,
            counter,
            samples: VecDeque::new(),
            started,
            paused_since: None,
        }
    }

    /// Stop counting the interval that starts now: the receiver owes the covering proof,
    /// so a delivery pause here is self-inflicted, not the sender's stall.
    pub(crate) const fn pause(&mut self, now: Instant) {
        if self.paused_since.is_none() {
            self.paused_since = Some(now);
        }
    }

    /// Resume counting. The paused span is folded out of the timeline: retained samples
    /// and the warmup origin shift forward by the span, so the pause reads as no elapsed
    /// time rather than as a stall.
    pub(crate) fn resume(&mut self, now: Instant) {
        if let Some(since) = self.paused_since.take() {
            let span = now.saturating_duration_since(since);
            for sample in &mut self.samples {
                if let Some(shifted) = sample.0.checked_add(span) {
                    sample.0 = shifted;
                }
            }
            if let Some(shifted) = self.started.checked_add(span) {
                self.started = shifted;
            }
        }
    }

    /// Sample the counter and judge the trailing window. Returns [`FloorVerdict::Stalled`]
    /// only once the window is warm and the bytes across it fall below `floor_bps · window`
    /// (or below one byte when `floor_bps == 0`).
    pub(crate) fn evaluate(&mut self, now: Instant) -> FloorVerdict {
        if self.paused_since.is_some() {
            return FloorVerdict::Ok;
        }
        let current = self.counter.load(Ordering::Relaxed);
        self.samples.push_back((now, current));
        let window = self.cfg.window;
        // Drop samples older than the window, but keep the newest one at or before the
        // window boundary as the baseline so the delta spans the whole window.
        if let Some(boundary) = now.checked_sub(window) {
            while self.samples.len() >= 2 {
                match self.samples.get(1) {
                    Some(&(t1, _)) if t1 <= boundary => {
                        self.samples.pop_front();
                    }
                    _ => break,
                }
            }
        }
        // The window is not judgeable until a full window of streaming has elapsed; this
        // grace is the first-byte time budget.
        if now.saturating_duration_since(self.started) < window {
            return FloorVerdict::Ok;
        }
        let baseline = self.samples.front().map_or(current, |&(_, v)| v);
        let delta = current.saturating_sub(baseline);
        if delta < required_bytes(window, self.cfg.floor_bps) {
            FloorVerdict::Stalled
        } else {
            FloorVerdict::Ok
        }
    }
}

/// Bytes that must cross the window to clear the floor: `floor_bps · window`, never below
/// one byte so `floor_bps == 0` still catches a full wedge.
fn required_bytes(window: Duration, floor_bps: u64) -> u64 {
    let req = u128::from(floor_bps).saturating_mul(window.as_millis()) / 1000;
    u64::try_from(req).unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod floor_tests {
    use super::*;

    fn cfg(floor_bps: u64) -> FloorConfig {
        FloorConfig {
            window: Duration::from_secs(20),
            floor_bps,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_throughput_never_stalls() {
        let counter = Arc::new(AtomicU64::new(0));
        let t0 = Instant::now();
        let mut f = ThroughputFloor::new(cfg(4096), Arc::clone(&counter), t0);
        for s in 1..=60u64 {
            counter.store(100 * 1024 * s, Ordering::Relaxed);
            let v = f.evaluate(t0 + Duration::from_secs(s));
            assert_eq!(v, FloorVerdict::Ok, "stalled at {s}s");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slow_drip_below_floor_stalls_after_window() {
        let counter = Arc::new(AtomicU64::new(0));
        let t0 = Instant::now();
        let mut f = ThroughputFloor::new(cfg(4096), Arc::clone(&counter), t0);
        let mut verdict = FloorVerdict::Ok;
        for s in 1..=25u64 {
            counter.store(100 * s, Ordering::Relaxed);
            verdict = f.evaluate(t0 + Duration::from_secs(s));
        }
        assert_eq!(verdict, FloorVerdict::Stalled);
    }

    #[tokio::test(start_paused = true)]
    async fn full_wedge_stalls_with_floor_zero() {
        let counter = Arc::new(AtomicU64::new(1)); // one byte then silence
        let t0 = Instant::now();
        let mut f = ThroughputFloor::new(cfg(0), Arc::clone(&counter), t0);
        let mut verdict = FloorVerdict::Ok;
        for s in 1..=21u64 {
            verdict = f.evaluate(t0 + Duration::from_secs(s));
        }
        assert_eq!(verdict, FloorVerdict::Stalled);
    }

    #[tokio::test(start_paused = true)]
    async fn payment_pause_is_not_charged_to_sender() {
        let counter = Arc::new(AtomicU64::new(1_000_000));
        let t0 = Instant::now();
        let mut f = ThroughputFloor::new(cfg(4096), Arc::clone(&counter), t0);
        for s in 1..=5u64 {
            counter.store(1_000_000 + 100 * 1024 * s, Ordering::Relaxed);
            f.evaluate(t0 + Duration::from_secs(s));
        }
        f.pause(t0 + Duration::from_secs(5));
        let v = f.evaluate(t0 + Duration::from_secs(35));
        f.resume(t0 + Duration::from_secs(35));
        assert_eq!(v, FloorVerdict::Ok);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn counter_advances_by_bytes_read() -> std::io::Result<()> {
        let mut src: &[u8] = b"hello world"; // 11 bytes
        let counter = Arc::new(AtomicU64::new(0));
        let mut r = ProgressReader::new(&mut src, Arc::clone(&counter));
        let mut buf = [0u8; 5];
        r.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello");
        assert_eq!(counter.load(Ordering::Relaxed), 5);
        let mut rest = Vec::new();
        r.read_to_end(&mut rest).await?;
        assert_eq!(counter.load(Ordering::Relaxed), 11);
        Ok(())
    }
}
