//! Time-to-first-byte phase marks for one `decdn fetch`.
//!
//! [`FetchTimings`] records when the fetch first reaches each [`Mark`] and
//! logs every mark as milliseconds since the fetch started, in one `info`
//! event named `fetch timings`. `RUST_LOG=info` or `-v` shows it. Each mark is
//! cumulative, so a mark the fetch never reached shows as absent and the marks
//! before it still read correctly.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// One point on the fetch's path to its first byte, in path order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mark {
    /// The iroh endpoint is bound.
    Endpoint,
    /// Discovery starts its probe round. Absent when the peer store's fresh
    /// latencies or `--node-id` choose the holders without a probe.
    ProbeStart,
    /// The first holder set is resolved.
    Resolved,
    /// The keystore signer is unlocked.
    Unlocked,
    /// The first lane's pool is open or reused and its source is ready.
    Lane,
    /// The first verified content byte reaches the user: written to stdout, or
    /// counted in the file download's progress.
    FirstByte,
}

/// The first instant of each [`Mark`] in one fetch. Shared by reference; each
/// mark keeps its first instant.
#[derive(Debug)]
pub(crate) struct FetchTimings {
    started: Instant,
    marks: [OnceLock<Instant>; 6],
}

/// Each [`Mark`] as milliseconds since the fetch started, `None` when the
/// fetch never reached it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) endpoint: Option<u64>,
    pub(crate) probe_start: Option<u64>,
    pub(crate) resolved: Option<u64>,
    pub(crate) unlocked: Option<u64>,
    pub(crate) lane: Option<u64>,
    pub(crate) first_byte: Option<u64>,
}

impl FetchTimings {
    /// Timings for a fetch that starts now.
    pub(crate) fn start() -> Self {
        Self::started_at(Instant::now())
    }

    /// Timings for a fetch that started at `started`.
    fn started_at(started: Instant) -> Self {
        Self {
            started,
            marks: Default::default(),
        }
    }

    /// Record `mark` now, unless the fetch already reached it.
    pub(crate) fn mark(&self, mark: Mark) {
        self.mark_at(mark, Instant::now());
    }

    fn mark_at(&self, mark: Mark, at: Instant) {
        if let Some(slot) = self.marks.get(mark as usize) {
            let _ = slot.set(at);
        }
    }

    fn ms(&self, mark: Mark) -> Option<u64> {
        let at = self.marks.get(mark as usize)?.get()?;
        Some(millis(at.saturating_duration_since(self.started)))
    }

    /// Every mark as milliseconds since the fetch started.
    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            endpoint: self.ms(Mark::Endpoint),
            probe_start: self.ms(Mark::ProbeStart),
            resolved: self.ms(Mark::Resolved),
            unlocked: self.ms(Mark::Unlocked),
            lane: self.ms(Mark::Lane),
            first_byte: self.ms(Mark::FirstByte),
        }
    }

    /// Log every mark and the total so far in one `fetch timings` event.
    /// `output` names where the bytes go, and `ok` is the fetch's outcome.
    pub(crate) fn log(&self, output: &'static str, ok: bool) {
        let s = self.snapshot();
        tracing::info!(
            output,
            ok,
            endpoint_ms = s.endpoint,
            probe_start_ms = s.probe_start,
            resolved_ms = s.resolved,
            unlocked_ms = s.unlocked,
            lane_ms = s.lane,
            first_byte_ms = s.first_byte,
            total_ms = millis(self.started.elapsed()),
            "fetch timings"
        );
    }
}

/// `d` in whole milliseconds, saturating at `u64::MAX`.
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_mark_keeps_its_first_instant() {
        let started = Instant::now();
        let timings = FetchTimings::started_at(started);
        timings.mark_at(Mark::Resolved, started + Duration::from_millis(40));
        timings.mark_at(Mark::Resolved, started + Duration::from_millis(90));
        timings.mark_at(Mark::FirstByte, started + Duration::from_millis(250));

        let s = timings.snapshot();
        assert_eq!(s.resolved, Some(40));
        assert_eq!(s.first_byte, Some(250));
    }

    #[test]
    fn an_unreached_mark_is_absent() {
        let started = Instant::now();
        let timings = FetchTimings::started_at(started);
        timings.mark_at(Mark::Endpoint, started + Duration::from_millis(3));

        let s = timings.snapshot();
        assert_eq!(s.endpoint, Some(3));
        assert_eq!(s.probe_start, None);
        assert_eq!(s.lane, None);
        assert_eq!(s.first_byte, None);
    }
}
